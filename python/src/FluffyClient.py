import	socket
import	struct
import	time
import common_pb2
import	pipe_pb2
import	pbd
from protobuf_to_dict import protobuf_to_dict
from asyncore import read
from Logger import Logger
from ClientServerMessageBuilder import ClientServerMessageBuilder

class FluffyClient:
    def __init__(self, host, portNumber):
        self.host = host
        self.portNumber = portNumber
        self.nodeId = 999
        self.time = 20000
        self.myIp = "127.0.0.1"
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.logger = Logger("fluffyClient.log")
        self.clientServerMessageBuilder = ClientServerMessageBuilder()
        self.logger.logInfoMessage("Client: Created a Fluffy client object")

    def setName(self, name):
        self.name = name

    def getName(self):
        return self.name

    def openConnection(self):
        self.socket.connect((self.host, self.portNumber))
        self.logger.logInfoMessage("Client: Connected to a server socket using address: " + self.host + " and port: " + str(self.portNumber))
        print("Client: Connected to a server socket using address: " + self.host + " and port: " + str(self.portNumber))

    def closeConnection(self):
        connectionStopMsg = self.clientServerMessageBuilder.buildGeneralMessage(self.nodeId, self.time, self.name + " is closing the session!!!")
        self.logger.logInfoMessage("Client: Client closed the connection with the server: " + connectionStopMsg)
        print("Client: Client closed the connection with the server: " + connectionStopMsg)
        self.socket.send(connectionStopMsg)
        self.socket.close()
        self.socket = None

    def sendConnectedInfoToServer(self):
        newConnectionInfoMsg = self.clientServerMessageBuilder.buildGeneralMessage(self.nodeId, self.time, self.name + " is connected to the server")
        self.socket.send(newConnectionInfoMsg)
        self.logger.logInfoMessage("Client: Client is connected with the server: " + newConnectionInfoMsg)
        print("Client: Client is connected with the server: " + newConnectionInfoMsg)

    def sendMessageToServer(self, message):
        if len(message) > 1024:
            print('Client: message exceeds 1024 size')
            return
        msgToServer = self.clientServerMessageBuilder.buildGeneralMessage(self.nodeId, self.time, self.name + " says: " + message)
        self.socket.send(msgToServer)
        self.logger.logInfoMessage("Client: Client is sending a message to server: " + msgToServer)
        print("Client: Client is sending a message to server: " + msgToServer)

    def chunkFileInto1MB(self, file):
        oneMBfileChunks = []
        fileReadSize = 1024 * 1024
        with open(file, "rb") as file:
            dataRead = "data"
            while dataRead != '':
                dataRead = file.read(fileReadSize)
                oneMBfileChunks.append(dataRead)
            return oneMBfileChunks

    def _sendCommandMessage(self, commandMessage):
        messageLength = struct.pack('>L', len(commandMessage))
        self.socket.sendall(messageLength + commandMessage)
        self.logger.logInfoMessage("Client: Client is sending data to server: " + messageLength + commandMessage)
        print("Client: Client is sending data to server: " + messageLength + commandMessage)

    def _recvCommandMessage(self):
        lenOfReceivedMsg = self.receiveMessageFromServer(self.socket, 4)
        messageReceived = self.receiveMessageFromServer(self.socket, lenOfReceivedMsg)
        readCommandMessage = pipe_pb2.CommandMessage()
        readCommandMessage.ParseFromString(messageReceived)
        self.logger.logInfoMessage("Client: Client received message from server: " + readCommandMessage)
        print("Client: Client received message from server: " + readCommandMessage)
        # self.socket.close
        return readCommandMessage

    def sendFileToServer(self, filename, chunkCount, chunkId, fileChunk):
        newCommandMsg = self.clientServerMessageBuilder.buildWriteCommandMessage(self.nodeId, self.time, self.myIp, filename, chunkCount, chunkId, fileChunk)
        self._sendCommandMessage(newCommandMsg)
        return self._recvCommandMessage()

    def getFileFromServer(self, filename):
        newReadCommandMessage = self.clientServerMessageBuilder.buildReadCommandMessage(self.nodeId, self.time, self.myIp, filename)
        self._sendCommandMessage(newReadCommandMessage)
        return self._recvCommandMessage()

    def receiveMessageFromServer(self, socket, waitFor):
        socket.setblocking(0)
        fileContents = []
        fileSize = 8192
        data = ''
        startTime = time.time()
        while 1:
            if data and time.time() - startTime > waitFor:
                break
            elif time.time() - startTime > (waitFor * 2):
                break
        try:
            data = socket.recv(fileSize)
            if data:
                fileContents.append(data)
                begin = time.time()
            else:
                time.sleep(0.1)
        except:
            pass
            self.logger.logInfoMessage("Client: file contents received: " + str(fileContents))
            print("Client: file contents received: " + str(fileContents))
            return ''.join(fileContents)