#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <pthread.h>
#include <string.h>

// add anagram functionality later

void *receive_results(void *arg){
    FILE *g = (FILE *)arg;
    int *result;
    while(1){
        MPI_recv(result,1,MPI_INT,MPI_ANY_SOURCE,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        fprintf(g,"%d\n",*result);
    }

    return NULL;
}


void main_server(FILE *f, FILE *g){
    char command[100];
    char *token,*request,*request_argument;
    int sleep_amount,request_N;
    pthread_t th;
    
    if(pthread_create(th,NULL,receive_results,(void *)g) != 0){
        perror("creating thread");
        exit(1);
    }

    while(fgets(command,99,f) != NULL){
        token =  strtok(command," ");
        if(strcmp(token, "WAIT") == 0){
            token =  strtok(NULL," ");
            sleep_amount = atoi(token);
            sleep(sleep_amount);
        }
        else{
            request = strtok(NULL," ");
            request_argument = strtok(NULL, " ");
            if(strcmp(request,"PRIMEDIVISORS") == 0 || strcmp(request, "PRIMES") == 0){
                request_N = atoi(request_argument);
            }
            else if(strcmp(request,"ANAGRAMS")){
                printf("Unknown command %s %s %s",token, request, request_argument);
            }
        }
    }

    pthread_join(th,NULL);
}

void worker_server(){}

int main(int argc, char **argv){
    int my_rank, comm_sz;
    FILE *f,*g;
    if(argc < 2){
        printf("No input file\n");
        exit(1);
    }


    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    if(my_rank == 0){
        if((f = fopen(argv[1],"r")) == NULL){
            printf("Problem opening input file\n");
            exit(1);
        }
        if((g = fopen("output.txt","w")) == NULL){
            printf("Problem opening output file\n");
            exit(1);
        }
        main_server(f,g);
        fclose(f);
        fclose(g);
    }
    else
        worker_server();

    MPI_Finalize();
    
    return 0;
}