import sys
from Logger import Logger
from FluffyClient import FluffyClient

class FluffyClientApplication:
    def __init__(self):
        self.name = None
        self.host = None
        self.port = None
        self.fluffyClient = None
        pass

    def _printHeaderMessage(self, name):
        print("\n")
        print("#########################################################################################################")
        print("###################################Fluffy Client Application#############################################")
        print("#########################################################################################################")
        print("\n")

    def _printClosingMessage(self, name):
        print("\n")
        print("#########################################################################################################")
        print("##############################Thank you for choosing Fluffy, " + name + " ###############################")
        print("#########################################################################################################")
        print("\n")

    def _printHelpContent(self):
        print("Select an option by using the option number tagged along with the option: ")
        print("**************************************************************************\n")
        print("(1) upload - uploads a file into fluffy server\n")
        print("(2) download - downloads a file from fluffy server\n")
        print("(3) help - prints all the options present\n")
        print("(4) exit - ends the application\n")
        print("**************************************************************************\n")
    def start(self):
        self.name = None
        self.host = None
        self.port = None
        self.fluffyClient = None
        while self.name == None or self.host == None or self.port == None:
            if(self.name == None):
                print("Enter your name to join the connection: ")
                self.name = sys.stdin.readline()
                if(self.name == None):
                    continue
            if(self.host == None):
                print("Enter the IP of Server to connect: ")
                self.host = raw_input()
                if(self.host == None):
                    continue
            if(self.port == None):
                print("Enter the port of server: ")
                self.port = int(raw_input())
                if(self.port == None):
                    continue
            if(self.name != None and self.host != None and self.port != None):
                break
        self._printHeaderMessage(self.name)
        self.fluffyClient = FluffyClient(self.host, self.port)
        self.fluffyClient.openConnection()
        self._printHelpContent()
        while(True):
            optionSelected = int(raw_input())
            ##
            # storing the file in Fluffy
            ##
            if (optionSelected == 1):
                print("Enter the name of your file (eg: test.txt): ")
                filename = raw_input()
                #bc.sendData(bc.genPing(),"127.0.0.1",4186);
                print("Enter path name of the file (eg: '/home/abcd/folder'): ")
                path = raw_input()
                #path = "introductions3.pdf";
                fileChunks = self.fluffyClient.chunkFileInto1MB(filename)
                chunkCount = len(fileChunks)
                chunkId = 1
                for fileChunk in fileChunks:
                    serverResponse = self.fluffyClient.sendFileToServer(filename, chunkCount, chunkId, fileChunk)
                    chunkId += 1
            ##
            # storing the file in Fluffy
            ##
            elif (optionSelected == 2):
                print("Enter the filename you want to download from Fluffy: ")
                filename = raw_input()
                print("The file if present will be downloaded to same folder")
                fileContents = self.fluffyClient.getFileFromServer(filename)
                if(fileContents.fileTask.filename == filename):
                    chunkCount = fileContents.fileTask.chunk_counts
                    while chunkCount != 0:
                        if (fileContents.fileTask.filename == filename):
                            with open(filename, "w") as outfile:
                                outfile.write(fileContents.fileTask.chunk)

            ##
            # displaying the commands
            ##
            elif (optionSelected == 3):
                self._printHelpContent()
            ##
            # exiting the client application
            ##
            elif (optionSelected == 4):
                self.fluffyClient.closeConnection()
                self._printClosingMessage(self.name)
                break
            else:
                print("Error: Incorrect option selected: " + str(optionSelected) + " ,option should be in range (1 - 4)")

if __name__ == "__main__":
    fluffyApp = FluffyClientApplication()
    fluffyApp.start()