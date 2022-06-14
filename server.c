/**
 * NTU System Programming, Fall 2021, Programming Assignment #1: csieVaccination
 * 
 * @author rogeryen
 * @date   June 14th, 2022
 */
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

// ============================ Error Handlers ============================

/* Handle usage errors. */
static void
usageErr(const char *progName) {
        fprintf(stderr, "usage: %s port\n", progName);
        exit(EXIT_FAILURE);
}

/* Handle system/function call errors. */
static void
errExit(const char *errMsg) {
        perror(errMsg);
        exit(EXIT_FAILURE);
}

// ========================================================================
// ============================== Structures ==============================

typedef struct {
        int id;                    /* 902001 - 902020 */
        int AZ;
        int BNT;
        int Moderna;
} RegisterRecord;

typedef struct {
        char hostname[512];        /* Server's hostname. */
        unsigned short port;       /* Port to listen. */
        int listenFd;              /* Fildes to wait for a new connection. */
} Server;

typedef struct {
        char host[512];            /* Client's host. */
        int connFd;                /* Fildes to talk with client. */
        char buf[512];             /* Data sent by/to client. */
        size_t bufLen;             /* Number of bytes used by buf. */

        int id;                    /* User id. */
        int waitForWrite;          /* Used for checking which phase of input is currently being handled. */
        RegisterRecord record;     /* The corresponding registration record. */
} Request;

// ========================================================================
// ========================== Function Prototypes =========================

/* Initialize a server, exiting if errors occur. */
static void initServer(unsigned short port);

/* Initialize a request instance. */
static void initRequest(Request *reqP);

/* Free resources used by a request instance. */
static void freeRequest(Request *reqP);

/* Handle input from a request instance. */
static int handleInput(Request *reqP);

/* Send message to client. */
static void sendMsg(Request *reqP, const char *msg);

/* Close the connection from client. */
static void disconnect(Request *reqP);

/* Check the validity of user's id. */
static int idIsValid(Request *reqP);

/* Attempt to set lock on the "registerRecord" file, returning -1 if an error occurs. */
static int setLock(Request *reqP, short lockType);

/* Send the client his/her preference order. */
static void sendOrder(Request *reqP);

/* Send the client his/her new preference order, while updating it at the same time. */
static void updateAndSendOrder(Request *reqP);

// ========================================================================

Server svr;
Request *requestP;
int maxFd;

int fileFd;
fd_set mset, wset;
int wrLocked[20] = {0};

// ============================= Main Function ============================

int
main(int argc, char *argv[]) {
        /* Parse arguments. */
        if (argc != 2) {
                usageErr(argv[0]);
        }

        /* Initialize server. */
        unsigned short port = (unsigned short) strtol(argv[1], NULL, 10);
        initServer(port);

        /* Initialize fd_set's. */
        FD_ZERO(&mset);
        FD_SET(svr.listenFd, &mset);

        /* Open "registerRecord" file. */
        fileFd = open("registerRecord", O_RDWR);
        if (fileFd == -1) {
                errExit("open");
        }

        /* Loop for handling connections. */
        fprintf(stderr, "\nstarting on %.80s, port %d, fd %d, maxconn %d...\n", svr.hostname, svr.port, svr.listenFd, maxFd);
        while (1) {
                /* Apply I/O multiplexing. */
                memcpy(&wset, &mset, sizeof(mset));
                if (select(maxFd, &wset, NULL, NULL, NULL) == -1) {
                        errExit("select");
                }

                int connFd;  /* Fildes for a new connection with client. */

                if (FD_ISSET(svr.listenFd, &wset)) {
                        struct sockaddr_in cliaddr;
                        socklen_t clilen = sizeof(cliaddr);

                        /* Check new connection. */
                        connFd = accept(svr.listenFd, (struct sockaddr *) &cliaddr, &clilen);
                        if (connFd == -1) {
                                if (errno == EINTR || errno == EAGAIN || errno == ENFILE) {
                                        if (errno == ENFILE) {
                                                fprintf(stderr, "out of file descriptor table ... (maxconn %d)\n", maxFd);
                                        }
                                        continue;
                                }
                                errExit("accept");
                        }

                        requestP[connFd].connFd = connFd;
                        strcpy(requestP[connFd].host, inet_ntoa(cliaddr.sin_addr));
                        fprintf(stderr, "getting a new request... fd %d from %s\n", connFd, requestP[connFd].host);

                        FD_SET(connFd, &mset);
                        sendMsg(&requestP[connFd], "Please enter your id (to check your preference order):\n");
                        continue;
                }

                /* Find a client that is ready. */
                connFd = -1;
                for (int i = 3; i < maxFd; i++) {
                        if (i != svr.listenFd && FD_ISSET(i, &wset)) {
                                connFd = i;
                                break;
                        }
                }
                if (connFd == -1) {
                        continue;
                }

                Request *reqP = &requestP[connFd];

                /* Handle request from client. */
                int ret = handleInput(reqP);
                if (ret == -1) {
                        fprintf(stderr, "bad request from %s\n", (*reqP).host);
                        disconnect(reqP);
                        continue;
                }

#ifdef READ_SERVER

                if (!idIsValid(reqP)) {
                        sendMsg(reqP, "[Error] Operation failed. Please try again.\n");
                } else {
                        if (setLock(reqP, F_RDLCK) == -1) {
                                if (errno != EAGAIN) {
                                        errExit("fcntl");
                                }
                                sendMsg(reqP, "Locked.\n");
                        } else {
                                sendOrder(reqP);
                                if (setLock(reqP, F_UNLCK) == -1) {
                                        errExit("fcntl");
                                }
                        }
                }
                disconnect(reqP);

#elif defined WRITE_SERVER

                if ((*reqP).waitForWrite == 0) {
                        if (!idIsValid(reqP)) {
                                sendMsg(reqP, "[Error] Operation failed. Please try again.\n");
                                disconnect(reqP);
                        } else {
                                if (setLock(reqP, F_WRLCK) == -1) {
                                        if (errno != EAGAIN) {
                                                errExit("fcntl");
                                        }
                                        sendMsg(reqP, "Locked.\n");
                                        disconnect(reqP);
                                } else if (wrLocked[(*reqP).id - 902001]) {
                                        sendMsg(reqP, "Locked.\n");
                                        disconnect(reqP);
                                } else {
                                        wrLocked[(*reqP).id - 902001] = 1;
                                        (*reqP).waitForWrite = 1;

                                        sendOrder(reqP);
                                        sendMsg(reqP, "Please input your preference order respectively(AZ,BNT,Moderna):\n");
                                }
                        }
                } else {
                        updateAndSendOrder(reqP);

                        wrLocked[(*reqP).id - 902001] = 0;
                        if (setLock(reqP, F_UNLCK) == -1) {
                                errExit("fcntl");
                        }
                        disconnect(reqP);
                }
#endif
        }

        free(requestP);
        if (close(fileFd) == -1) {
                errExit("close");
        }

        exit(EXIT_SUCCESS);
}

// ========================================================================
// ======================== Function Implementations ======================

static void
initServer(unsigned short port) {
        struct sockaddr_in servaddr;
        int tmp;

        gethostname(svr.hostname, sizeof(svr.hostname));
        svr.port = port;
        svr.listenFd = socket(AF_INET, SOCK_STREAM, 0);
        if (svr.listenFd == -1) {
                errExit("socket");
        }

        bzero(&servaddr, sizeof(servaddr));
        servaddr.sin_family = AF_INET;
        servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
        servaddr.sin_port = htons(port);
        tmp = 1;

        if (setsockopt(svr.listenFd, SOL_SOCKET, SO_REUSEADDR, (void *) &tmp, sizeof(tmp)) == -1) {
                errExit("setsockopt");
        }
        if (bind(svr.listenFd, (struct sockaddr *) &servaddr, sizeof(servaddr)) == -1) {
                errExit("bind");
        }
        if (listen(svr.listenFd, 1024) == -1) {
                errExit("listen");
        }

        maxFd = getdtablesize();
        requestP = (Request *) malloc(sizeof(Request) *  maxFd);
        if (requestP == NULL) {
                errExit("malloc");
        }
        for (int i = 0; i < maxFd; i++) {
                initRequest(&requestP[i]);
        }
        requestP[svr.listenFd].connFd = svr.listenFd;
        strcpy(requestP[svr.listenFd].host, svr.hostname);
}

static void
initRequest(Request *reqP) {
        reqP->connFd = -1;
        reqP->bufLen = 0;
        reqP->id = 0;
        reqP->waitForWrite = 0;
        memset(&reqP->record, 0, sizeof(RegisterRecord));
}

static void
freeRequest(Request *reqP) {
        initRequest(reqP);
}

static int
handleInput(Request *reqP) {
        ssize_t numRead;
        char buf[512];
        if ((numRead = read(reqP->connFd, buf, sizeof(buf))) <= 0) {
                return (int) numRead;
        }

        char *ptr = strstr(buf, "\r\n");
        if (ptr == NULL) {
                ptr = strstr(buf, "\n");
                if (ptr == NULL) {
                        fprintf(stderr, "This really shouldn't have happened.\n");
                        exit(EXIT_FAILURE);
                }
        }

        size_t bufLen = ptr - buf + 1;
        memmove(reqP->buf, buf, bufLen);
        reqP->buf[bufLen - 1] = '\0';
        reqP->bufLen = bufLen - 1;

        return 1;
}

static inline void
sendMsg(Request *reqP, const char *msg) {
        if (write(reqP->connFd, msg, strlen(msg)) != strlen(msg)) {
                errExit("write");
        }
}

static void
disconnect(Request *reqP) {
        FD_CLR(reqP->connFd, &mset);
        if (close(reqP->connFd) == -1) {
                errExit("close");
        }
        freeRequest(reqP);
}

static int
idIsValid(Request *reqP) {
        if (reqP->bufLen == 6) {
                if (strcmp("902001", reqP->buf) <= 0 && strcmp(reqP->buf, "902020") <= 0) {
                        reqP->id = (int) strtol(reqP->buf, NULL, 10);
                        return 1;
                }
        }
        return 0;
}

static int
setLock(Request *reqP, short lockType) {
        struct flock lock;
        lock.l_type = lockType;
        lock.l_whence = SEEK_SET;
        lock.l_start = (reqP->id - 902001) * (off_t) sizeof(RegisterRecord);
        lock.l_len = sizeof(RegisterRecord);

        return fcntl(fileFd, F_SETLK, &lock);
}

static void
sendOrder(Request *reqP) {
        if (lseek(fileFd, (reqP->id - 902001) * (off_t) sizeof(RegisterRecord), SEEK_SET) == -1) {
                errExit("lseek");
        }
        if (read(fileFd, &reqP->record, sizeof(reqP->record)) != sizeof(reqP->record)) {
                errExit("read");
        }

        char buf[512];
        sprintf(buf, "Your preference order is %s > %s > %s.\n", 
                        reqP->record.AZ == 1 ? "AZ" : reqP->record.BNT == 1 ? "BNT" : "Moderna", 
                        reqP->record.AZ == 2 ? "AZ" : reqP->record.BNT == 2 ? "BNT" : "Moderna", 
                        reqP->record.AZ == 3 ? "AZ" : reqP->record.BNT == 3 ? "BNT" : "Moderna"
        );
        sendMsg(reqP, buf);
}

char validOrders[6][3] = {{'1', '2', '3'}, 
                          {'1', '3', '2'}, 
                          {'2', '1', '3'}, 
                          {'2', '3', '1'}, 
                          {'3', '1', '2'}, 
                          {'3', '2', '1'}};

static void
updateAndSendOrder(Request *reqP) {
        if (reqP->bufLen == 5) {
                char s1[512], s2[512], s3[512];

                if (sscanf(reqP->buf, "%s%s%s", s1, s2, s3) == 3) {
                        if (strlen(s1) == 1 && strlen(s2) == 1 && strlen(s3) == 1) {
                                if (isdigit(s1[0]) && isdigit(s2[0]) && isdigit(s3[0])) {
                                        int isValid = 0;
                                        for (int i = 0; i < 6; i++) {
                                                if (s1[0] == validOrders[i][0] && s2[0] == validOrders[i][1] && s3[0] == validOrders[i][2]) {
                                                        isValid = 1;
                                                        break;
                                                }
                                        }

                                        if (isValid) {
                                                reqP->record.AZ = s1[0] - '0';
                                                reqP->record.BNT = s2[0] - '0';
                                                reqP->record.Moderna = s3[0] - '0';

                                                if (lseek(fileFd, (reqP->id - 902001) * (off_t) sizeof(reqP->record), SEEK_SET) == -1) {
                                                        errExit("lseek");
                                                }
                                                if (write(fileFd, &reqP->record, sizeof(reqP->record)) != sizeof(reqP->record)) {
                                                        errExit("write");
                                                }

                                                sprintf(s1, "Preference order for %d modified successed, new preference order is %s > %s > %s.\n",
                                                                reqP->id,
                                                                reqP->record.AZ == 1 ? "AZ" : reqP->record.BNT == 1 ? "BNT" : "Moderna",
                                                                reqP->record.AZ == 2 ? "AZ" : reqP->record.BNT == 2 ? "BNT" : "Moderna", 
                                                                reqP->record.AZ == 3 ? "AZ" : reqP->record.BNT == 3 ? "BNT" : "Moderna"
                                                );
                                                sendMsg(reqP, s1);
                                                return;
                                        }
                                }
                        }
                }
        }
        sendMsg(reqP, "[Error] Operation failed. Please try again.\n");
}
