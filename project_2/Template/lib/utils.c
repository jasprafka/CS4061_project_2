#include "utils.h"

char *getChunkData(int mapperID) {

	// open the message queue
    int mid;
    key_t key = 100;
	// if this code fails, try uncommenting the following line and commenting out the one after
	// mid = msgget(key, 0666);
    mid = msgget(key, 0666 | IPC_CREAT);
    if(mid == -1) {
		printf("ERROR: failed to open message queue\n");
	    exit(1);
    }

  //TODO receive chunk from the master
  // When you receive the chunk, the valid portion of the chunk data will be up to the first '\0' char
  // i.e the last chunk may not have a full 1024 valid bytes, it may have fewer
  // the last valid byte will be just before '\0'
    msgBuffer *buf = (msgBuffer *)malloc(sizeof(msgBuffer));
	
    if((msgrcv(mid, (void*)buf, sizeof(msgBuffer), mapperID, 0)) == -1){
	printf("Error: failed to read message in getChunkData\n");
    }

    char * chunkPtr = (char *)malloc(sizeof(msgBuffer)+1);
	
    strcpy(chunkPtr, buf->msgText);
	
  //TODO check for END message and send ACK to master and return NULL. 
  // When you send ACK message to master, msgType should be set equal to nMappers + 1
  //Otherwise return pointer to the chunk data. 
    if(buf->msgText[0] == 'E' && buf->msgText[1] == 'N' && buf->msgText[2] == 'D'){
	buf->msgText[0] = 'A';
	buf->msgText[1] = 'C';
	buf->msgText[2] = 'K';
	buf->msgType = 100;
	    
	if((msgsnd(mid, (void*)buf, sizeof(buf), 0)) == -1){
		printf("Error: failed to send message in getChunkData\n");
	}
	free(buf);
	return NULL;
    }

    free(buf);
    return chunkPtr;
}

void sendChunkData(char *inputFile, int nMappers) {
	
	/* 
		!!!!!!!!!!!!!! Don't forget to remove the test file code from this fn !!!!!!!!!!!!!! 
	*/

	int mapperID = 0, lseekFailed = 0;
	
	// open the message queue
    int mid;
    key_t key = 100;
    mid = msgget(key, 0666 | IPC_CREAT);
    if(mid == -1) {
		printf("ERROR: failed to open message queue\n");
	    exit(1);
    } 

	// Create buffer for sending messages to message queue
    msgBuffer *buf = (msgBuffer *)malloc (sizeof(msgBuffer));
    
	// Open input file
	int fd = open(inputFile, O_RDONLY);

	// open file for testing
	// FILE *writeTest;
	// writeTest = fopen("./writeTest.txt", "w");

	// Iterate over input data, sending 1024 byte chunks to each mapper
	int readCode;
	while((readCode = read(fd, buf->msgText, 1024)) > 0){

		if(readCode == -1) {
			printf("ERROR: failed to read from file\n");
			break;
		}

		// Test condition for wonky input data (i.e text w/ no ' ' or '\n')
		if(lseekFailed)
			break;
		
		buf->msgText[readCode] = '\0';
		
		// Assign chunks to mapperID in round robin fashion
		buf->msgType = (mapperID % nMappers) + 1; 
		mapperID++;

		// If this statement evaluates true, chunk truncated a word; 
		// reposition file pointer to previous ' ' or '\n'
		int i = readCode - 1;
		if((buf->msgText[i] != ' ') && (buf->msgText[i] != '\n')) {
				
			// Back up to previous ' ' or '\n'
			while ((buf->msgText[i] != ' ') && (buf->msgText[i] != '\n')) {
				i--;

				// Check for errors, and if file pointer has gone out of bounds
				if(lseek(fd, -1, SEEK_CUR) <= 0) {
					printf("ERROR: failed to reposition file offset\n");
					lseekFailed = 1;
					break;
				}
			}
			buf->msgText[i + 1] = '\0';
		}

		// Print to test file
		// fprintf(writeTest, "%s\n", buf->msgText);

		// Send message to mapper
		if(msgsnd(mid, buf, sizeof(msgBuffer), 0) == -1) {
			printf("ERROR: failed to send message\n");
			break;
		}
	}

	// close test file
	// fclose(writeTest);

	// Close file and ensure correct closure
	if(close(fd) == -1) {
		printf("ERROR: failed to close input file\n");
    }

    // inputFile read complete, send END message to each mapper
	char endMsg[MSGSIZE] = {'E', 'N', 'D'};
	sprintf(buf->msgText, "%s", endMsg);

	for (size_t i = 0; i < nMappers; i++)
	{
		buf->msgType = i + 1;
		if(msgsnd(mid, buf, sizeof(msgBuffer), 0) == -1) {
			printf("ERROR: failed to send message\n");
			break;
		}
	}

    //wait to receive ACK from all mappers for END notification
	int mappersDone = 0;
	while(mappersDone < nMappers) {
		if(msgrcv(mid, buf, sizeof(msgBuffer), 100 /*nMappers + 1*/, 0) != -1){ // Blocks until next mapper is done
			
			// Ensure ACK recieved
			if((buf->msgText[0] == 'A') && (buf->msgText[1] == 'C') && (buf->msgText[2] == 'K'))
				mappersDone++;

		} else {
			printf("ERROR: failed to read message\n");
		}
	}

	// free memory allocated for buffer
	free(buf);

}

// hash function to divide the list of word.txt files across reducers
//http://www.cse.yorku.ca/~oz/hash.html
int hashFunction(char* key, int reducers){
	unsigned long hash = 0;
    int c;

    while ((c = *key++)!='\0')
        hash = c + (hash << 6) + (hash << 16) - hash;

    return (hash % reducers);
}

int getInterData(char *key, int reducerID) {
    //TODO open message queue
      



    //TODO receive data from the master
     



    //TODO check for END message and send ACK to master and then return 0
    //Otherwise return 1


}

void shuffle(int nMappers, int nReducers) {

    //TODO open message queue
     


    //TODO traverse the directory of each Mapper and send the word filepath to the reducer
    //You should check dirent is . or .. or DS_Store,etc. If it is a regular
    //file, select the reducer using a hash function and send word filepath to
    //reducer 




    //TODO inputFile read complete, send END message to reducers

    


    
    //TODO  wait for ACK from the reducers for END notification
}

// check if the character is valid for a word
int validChar(char c){
	return (tolower(c) >= 'a' && tolower(c) <='z') ||
					(c >= '0' && c <= '9');
}

char *getWord(char *chunk, int *i){
	char *buffer = (char *)malloc(sizeof(char) * chunkSize);
	memset(buffer, '\0', chunkSize);
	int j = 0;
	while((*i) < strlen(chunk)) {
		// read a single word at a time from chunk
		// printf("%d\n", i);
		if (chunk[(*i)] == '\n' || chunk[(*i)] == ' ' || !validChar(chunk[(*i)]) || chunk[(*i)] == 0x0) {
			buffer[j] = '\0';
			if(strlen(buffer) > 0){
				(*i)++;
				return buffer;
			}
			j = 0;
			(*i)++;
			continue;
		}
		buffer[j] = chunk[(*i)];
		j++;
		(*i)++;
	}
	if(strlen(buffer) > 0)
		return buffer;
	return NULL;
}

void createOutputDir(){
	mkdir("output", ACCESSPERMS);
	mkdir("output/MapOut", ACCESSPERMS);
	mkdir("output/ReduceOut", ACCESSPERMS);
}

char *createMapDir(int mapperID){
	char *dirName = (char *) malloc(sizeof(char) * 100);
	memset(dirName, '\0', 100);
	sprintf(dirName, "output/MapOut/Map_%d", mapperID);
	mkdir(dirName, ACCESSPERMS);
	return dirName;
}

void removeOutputDir(){
	pid_t pid = fork();
	if(pid == 0){
		char *argv[] = {"rm", "-rf", "output", NULL};
		if (execvp(*argv, argv) < 0) {
			printf("ERROR: exec failed\n");
			exit(1);
		}
		exit(0);
	} else{
		wait(NULL);
	}
}

void bookeepingCode(){
	removeOutputDir();
	sleep(1);
	createOutputDir();
}
