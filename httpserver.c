#include "listener_socket.h"
#include "iowrapper.h"
#include "protocol.h"
#include "queue.h"
#include "rwlock.h"

#include <arpa/inet.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <regex.h>
#include <stdbool.h>
#include <pthread.h>
#include <unistd.h>

#define BUFFER_SIZE 4096
#define GET_PATTERN "^([a-zA-Z]{1,8}) (/[a-zA-Z0-9.-]{1,63}) (HTTP/[0-9]\\.[0-9])\r\n"

// linked list implementation for URI list
typedef struct uri_entry {
    char uri[64];
    rwlock_t *lock;
    struct uri_entry *next;
} uri_entry_t;

uri_entry_t *uri_list = NULL;
pthread_mutex_t list_mutex = PTHREAD_MUTEX_INITIALIZER;

// function to get a URI's specific lock
rwlock_t *get_uri_lock(char *uri) {
    pthread_mutex_lock(&list_mutex);
    uri_entry_t *current = uri_list;

    while (current != NULL) {
        if ((strcmp(current->uri, uri) == 0)) {
            pthread_mutex_unlock(&list_mutex);
            return current->lock;
        } else {
            current = current->next;
        }
    }

    // URI was not found, it MUST be a new entry
    uri_entry_t *new_node = malloc(sizeof(uri_entry_t));
    strncpy(new_node->uri, uri, 63);
    new_node->uri[63] = '\0';
    new_node->lock = rwlock_new(N_WAY, 5);

    new_node->next = uri_list;
    uri_list = new_node;
    pthread_mutex_unlock(&list_mutex);
    return new_node->lock;
}

queue_t *request_queue;

// function to get client's Request ID
// returns 0 if client didn't have a Request ID
long get_req_id(char *buf) {
    long req_id = 0;
    char *req = strstr(buf, "Request-Id:");
    if (req != NULL) {
        req += strlen("Request-Id:");
        while (*req == ' ') {
            req++;
        }
        req_id = strtol(req, NULL, 10);
    }
    return req_id;
}

void log_request(char *method, char *uri, int status, long request_id) {
    fprintf(stderr, "%s,/%s,%d,%ld\n", method, uri, status, request_id);
}

void invalidCommand(void) {
    fprintf(stderr, "Invalid Command\n");
}

void error_bad_request(int connfd) {
    char error[] = "HTTP/1.1 400 Bad Request\r\nContent-Length: 12\r\n\r\nBad Request\n";
    write_n_bytes(connfd, error, sizeof(error) - 1);
    return;
}

void error_internal(int connfd) {
    char error[]
        = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 22\r\n\r\nInternal Server Error\n";
    write_n_bytes(connfd, error, sizeof(error) - 1);
    return;
}

void error_not_found(int connfd) {
    char error[] = "HTTP/1.1 404 Not Found\r\nContent-Length: 10\r\n\r\nNot Found\n";
    write_n_bytes(connfd, error, sizeof(error) - 1);
    return;
}

void error_forbidden(int connfd) {
    char error[] = "HTTP/1.1 403 Forbidden\r\nContent-Length: 10\r\n\r\nForbidden\n";
    write_n_bytes(connfd, error, sizeof(error) - 1);
    return;
}

void write_func(size_t initial_read, size_t header_len, size_t remaining, int w_fd, int connfd,
    char buf[BUFFER_SIZE], long req_id, char *uri) {
    if (initial_read > header_len) {
        size_t buffered_data_len = initial_read - header_len;
        size_t to_write = (buffered_data_len < remaining) ? buffered_data_len : remaining;

        char *p = buf + header_len;
        size_t bytes_left = to_write;

        while (bytes_left > 0) {
            ssize_t bytes_written = write_n_bytes(w_fd, p, bytes_left);

            if (bytes_written < 0) {
                return;
            }

            p += bytes_written;
            bytes_left -= bytes_written;
        }
        remaining -= to_write;
    }

    char file_buf[BUFFER_SIZE];
    while (remaining > 0) {
        ssize_t to_read = 0;
        if (remaining < sizeof(file_buf)) {
            to_read = remaining;
        } else {
            to_read = sizeof(file_buf);
        }

        ssize_t bytes_read = read(connfd, file_buf, to_read);

        if (bytes_read < 0) {
            log_request("PUT", uri, 500, req_id);
            error_internal(connfd);
            return;
        }

        if (bytes_read == 0) {
            log_request("PUT", uri, 400, req_id);
            error_bad_request(connfd);
            return;
        }

        char *p = file_buf;
        ssize_t bytes_to_write = bytes_read;

        while (bytes_to_write > 0) {
            ssize_t bytes_written = write_n_bytes(w_fd, p, bytes_to_write);

            if (bytes_written < 0) {
                return;
            }

            p += bytes_written;
            bytes_to_write -= bytes_written;
        }

        remaining -= bytes_read;
    }
}

void get_logic(char buf[BUFFER_SIZE], regmatch_t pmatch[], int connfd) {
    long req_id = get_req_id(buf);
    buf[pmatch[2].rm_eo] = '\0';
    char *uri = buf + (1 + pmatch[2].rm_so);

    rwlock_t *lock = get_uri_lock(uri);
    reader_lock(lock);

    int read_fd = open(uri, O_RDONLY, 0);

    struct stat st;

    if (stat(uri, &st) == -1) {
        if (errno == ENOENT) {
            log_request("GET", uri, 404, req_id);
            error_not_found(connfd);
            reader_unlock(lock);
            return;
        } else if (errno == EACCES || errno == EISDIR || errno == ENOTDIR) {
            log_request("GET", uri, 403, req_id);
            error_forbidden(connfd);
            reader_unlock(lock);
            return;
        }
    }

    if (!S_ISREG(st.st_mode)) {
        log_request("GET", uri, 403, req_id);
        error_forbidden(connfd);
        reader_unlock(lock);
        return;
    }

    char ok_status[128];
    int len = snprintf(
        ok_status, sizeof(ok_status), "HTTP/1.1 200 OK\r\nContent-Length: %ld\r\n\r\n", st.st_size);
    write_n_bytes(connfd, ok_status, len);
    log_request("GET", uri, 200, req_id);

    char file_buffer[BUFFER_SIZE];
    while (1) {

        ssize_t read_res = read_n_bytes(read_fd, file_buffer, sizeof(file_buffer));

        if (read_res < 0) {
            error_internal(connfd);
            reader_unlock(lock);
            return;
        }

        if (read_res == 0) {
            break;
        }

        char *p = file_buffer;
        ssize_t bytes_to_write = read_res;

        while (bytes_to_write > 0) {
            ssize_t w_res = write_n_bytes(connfd, p, bytes_to_write);
            if (w_res < 0) {
                char error[] = "There is an error when writing the content of the given file\n";
                w_res = write_n_bytes(connfd, error, sizeof(error) - 1);
                reader_unlock(lock);
                return;
            }

            p += w_res;
            bytes_to_write -= w_res;
        }
    }

    reader_unlock(lock);
    close(read_fd);
    return;
}

void put_logic(char buf[BUFFER_SIZE], regmatch_t pmatch[], int connfd, int initial_read) {

    int uri_len = pmatch[2].rm_eo - pmatch[2].rm_so - 1;
    char uri[uri_len + 1];
    memcpy(uri, buf + (1 + pmatch[2].rm_so), uri_len);
    uri[uri_len] = '\0';
    long req_id = get_req_id(buf);

    rwlock_t *lock = get_uri_lock(uri);

    writer_lock(lock);

    char *cl = strstr(buf, "Content-Length:");

    if (cl == NULL) {
        log_request("PUT", uri, 400, req_id);
        error_bad_request(connfd);
        writer_unlock(lock);
        return;
    }

    cl += strlen("Content-Length:");
    while (*cl == ' ') {
        cl++;
    }

    char *end;
    long bytes_to_write_l = strtol(cl, &end, 10);

    if (bytes_to_write_l < 0) {
        log_request("PUT", uri, 400, req_id);
        error_bad_request(connfd);
        writer_unlock(lock);
        return;
    }

    size_t bytes_to_write = (size_t) bytes_to_write_l;

    if (bytes_to_write == 0) {
        log_request("PUT", uri, 400, req_id);
        error_bad_request(connfd);
        writer_unlock(lock);
        return;
    }

    struct stat st;
    bool existed = (stat(uri, &st) == 0);

    if (existed) {
        if (!S_ISREG(st.st_mode)) {
            log_request("PUT", uri, 403, req_id);
            error_forbidden(connfd);
            writer_unlock(lock);
            return;
        }
    }

    int write_fd = open(uri, O_WRONLY | O_CREAT | O_TRUNC, 0664);

    if (write_fd == -1) {
        if (errno == EACCES || errno == EISDIR) {
            error_forbidden(connfd);
            log_request("PUT", uri, 403, req_id);
            writer_unlock(lock);
            return;
        } else {
            error_internal(connfd);
            log_request("PUT", uri, 500, req_id);
            writer_unlock(lock);
            return;
        }
    }

    char *hdr_end = NULL;

    for (int i = 0; i <= initial_read - 4; i++) {
        if (buf[i] == '\r' && buf[i + 1] == '\n' && buf[i + 2] == '\r' && buf[i + 3] == '\n') {
            hdr_end = buf + i;
            break;
        }
    }

    if (hdr_end == NULL) {
        close(write_fd);
        log_request("PUT", uri, 400, req_id);
        error_bad_request(connfd);
        writer_unlock(lock);
        return;
    }

    ssize_t header_len = (hdr_end - buf) + 4;
    ssize_t remaining = bytes_to_write;

    write_func(initial_read, header_len, remaining, write_fd, connfd, buf, req_id, uri);
    close(write_fd);

    if (!existed) {
        char response[] = "HTTP/1.1 201 Created\r\nContent-Length: 8\r\n\r\nCreated\n";
        write_n_bytes(connfd, response, strlen(response));
        log_request("PUT", uri, 201, req_id);
    } else {
        char response[] = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nOK\n";
        write_n_bytes(connfd, response, strlen(response));
        log_request("PUT", uri, 200, req_id);
    }

    writer_unlock(lock);
    return;
}

void parse_through(char buf[BUFFER_SIZE], int connfd, int intial_read) {
    regex_t re;
    regmatch_t pmatch[4];
    regcomp(&re, GET_PATTERN, REG_EXTENDED);
    int reg_res = regexec(&re, buf, 4, pmatch, 0);

    // Bad Request Detected if no regex match is found
    if (reg_res != 0) {
        char error[] = "HTTP/1.1 400 Bad Request\r\nContent-Length: 12\r\n\r\nBad Request\n";
        write_n_bytes(connfd, error, sizeof(error) - 1);
        regfree(&re);
        return;
    }

    size_t version_len = pmatch[3].rm_eo - pmatch[3].rm_so;

    // An HTTP version mismatch has been detected
    if (version_len != 8 || strncmp(buf + pmatch[3].rm_so, "HTTP/1.1", 8) != 0) {
        char error[] = "HTTP/1.1 505 Version Not Supported\r\nContent-Length: 22\r\n\r\nVersion "
                       "Not Supported\n";
        write_n_bytes(connfd, error, sizeof(error) - 1);
        regfree(&re);
        return;
    }

    ssize_t input_len = pmatch[1].rm_eo - pmatch[1].rm_so;

    char *method = buf + pmatch[1].rm_so;
    char *uri = buf + (1 + pmatch[2].rm_so);
    long req_id = get_req_id(buf);

    // a PUT request has been detected
    if (input_len == 3 && strncmp(method, "PUT", 3) == 0) {
        put_logic(buf, pmatch, connfd, intial_read);
        // a GET request has been detected
    } else if (input_len == 3 && strncmp(method, "GET", 3) == 0) {
        get_logic(buf, pmatch, connfd);
        // another type of request has been detected that we cannot handle
    } else {
        char error[]
            = "HTTP/1.1 501 Not Implemented\r\nContent-Length: 16\r\n\r\nNot Implemented\n";
        write_n_bytes(connfd, error, sizeof(error) - 1);
        log_request(method, uri, 501, req_id);

        regfree(&re);
        return;
    }

    regfree(&re);
    return;
}

void handle_connection(int connfd, char *buffy) {
    // Initial Read for further parsing
    int total = 0;
    while (1) {
        ssize_t r_res = read(connfd, buffy + total, BUFFER_SIZE - total - 1);

        if (r_res <= 0) {
            return;
        }
        /*
        if (r_res == 0)
            break;
        */

        total += r_res;
        buffy[total] = '\0';

        if (strstr(buffy, "\r\n\r\n") != NULL) {
            break;
        }

        if (total >= BUFFER_SIZE - 1) {
            error_bad_request(connfd);
            return;
        }
    }
    parse_through(buffy, connfd, total);
    return;
}

// worker function thread to handle queue as well as handling specific client requests
void *worker_thread(void *argc) {
    (void) argc;
    char buf[BUFFER_SIZE];

    while (1) {
        void *elem;
        queue_pop(request_queue, &elem);
        int *fd_ptr = elem;
        int connfd = *fd_ptr;
        free(fd_ptr);

        memset(buf, 0, BUFFER_SIZE);
        handle_connection(connfd, buf);
        close(connfd);
    }
}
int main(int argc, char **argv) {
    int num_threads = 4;
    int c;

    while ((c = getopt(argc, argv, "t:")) != -1) {
        switch (c) {
        case 't': num_threads = strtol(optarg, NULL, 10); break;
        default: fprintf(stderr, "Invalid Input\n"); exit(1);
        }
    }

    if (optind >= argc) {
        fprintf(stderr, "Invalid Input\n");
        exit(1);
    }

    int port_num = strtol(argv[optind], NULL, 10);
    if (port_num < 1 || port_num > 65535) {
        fprintf(stderr, "Invalid Port\n");
        exit(1);
    }

    signal(SIGPIPE, SIG_IGN);
    Listener_Socket_t *socket = ls_new(port_num);

    if (socket == NULL) {
        fprintf(stderr, "Invalid Port\n");
        exit(1);
    }

    request_queue = queue_new(num_threads);

    pthread_t workers[num_threads];
    for (int i = 0; i < num_threads; i++) {
        pthread_create(&workers[i], NULL, worker_thread, NULL);
    }

    // Dispatcher thread loops forever
    while (1) {
        // Accept a new client connection and continue for proper handling
        int *fd_ptr = malloc(sizeof(int));
        int connection_socket = ls_accept(socket);
        *fd_ptr = connection_socket;
        queue_push(request_queue, fd_ptr);
    }

    return 0;
}
