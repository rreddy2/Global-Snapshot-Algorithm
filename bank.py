#!/usr/bin/env python

import os
import sys
import bank_pb2					# Used to access protobuf types
import socket					# Used to open TCP sockets
import time 					# Used for sleep()
import threading				# Used to create threads
from random import randint		# Used for generating random integers
from random import choice		# Used for generating random integers
from snapshot import snapshot
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')

# This global lock is used to access the balance attribute of a bank Object
threadLock = threading.Lock()

# This global lock is used to access the snapshots dictionary attribute of a 
# bank Object
snapDictionaryLock = threading.Lock()

# Class used to represent a thread that will periodically send transfer messages
# to other branches
class TransfersThread (threading.Thread):
	def __init__(self, branch):
		threading.Thread.__init__(self)
		self.branch = branch

	def run(self):
		try:
			self.branch.sendTransfers()
		except KeyboardInterrupt:
			print "Received KeyboardInterrupt"
			exit(1)



# Class used to represent a thread that will listen for incoming messages
# on a given socket (clientSocket)
class ListenerThread (threading.Thread):
	def __init__(self, branch, clientSocket):
		threading.Thread.__init__(self)
		self.branch = branch
		self.clientSocket = clientSocket

	def run(self):
		try:
			self.branch.acceptMessages(self.clientSocket)
		except KeyboardInterrupt:
			print "Received KeyboardInterrupt"
			exit(1)

# Class used to represent one branch of the bank
class bank:
	'''
	Attributes:
		- int balance		
			- Current balance of this branch. This may need to be accessed
			  by multiple concurrent threads
		- array branches	
			- Array of all other branches in the distributed bank.
			  Each element of the branches array is an object with 
			  three attributes: name (String), ip (String), and 
			  port (int) 
			  with three elements: branch name (String), ip address
			  (String), and port number (integer)	
		- String branch_name 
			- Name of this branch
		- socket socket	
			- Socket of this branch that is listening for incoming TCP 
			  connection requests
		- String ipAddress 
			- ip address of this branch
		- int portNumber 
			- port that this branch listens for incoming TCP connection
			  requests
		- int numOfMilliseconds
			- Max number of milliseconds between successive transfers that 
			  this bank sends out. After each transfer message to another branch,
			  calculate a random number in (0, numOfMilliseconds), waits that amount
			  of time, and sends a new transfer to another branch.
		- dict branchPorts
			- Dictionary where the keys are branch names (String) and 
			  the values are the corresponding TCP socket that this
			  branch can use to send a message to the other branch
		- dict snapshots
			- A dictionary where key is snapshot id and value is  Object of snapshot
			  class that is used to handle all of the 
			  functions necessary for the snapshot algorithm
		- bool sendingMarkers
			- Is True when this branch is sending out marker messages for a snapshot.
			- Is False otherwise
			- Ensures that Transfers aren't sent by this branch while marker messages
			  are being sent out.
	'''

	def __init__(self, name, portNumber, numberOfMilliseconds):
		self.balance = 0
		self.branches = []
		self.branch_name = name
		self.portNumber = portNumber
		self.numberOfMilliseconds = numberOfMilliseconds
		self.branchPorts = {}
		self.snapshots = {}
		self.sendingMarkers = False

		# Set up the TCP socket that this branch will listen to TCP 
		# connection requests on
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket.bind(('0.0.0.0', self.portNumber))
		self.socket.listen(5)
		self.ipAddress = socket.gethostbyname(socket.gethostname())

		print "(__init__) Ip of this Branch: " , self.ipAddress
		print "(__init__) Port Number of this Branch: ", self.socket.getsockname()[1]

	# Grabs the necessary lock, records the balance data memeber of this bank
	# Object, releases the lock, and returns the recorded balance
	def getBalance(self):
		threadLock.acquire(1)
		currentBalance = self.balance
		threadLock.release()
		return currentBalance

	def InitBranch(self, new_balance, new_branch_list):
		self.balance = new_balance
		for i in new_branch_list:
			self.branches.append(i)
		print "(InitBranch) Branch has been initialized"

		# Must open up TCP connections for this branch to send out transfers
		# to all other branches
		for branch in self.branches:
			# Don't open up a socket between this branch and itself
			if (branch.name == self.branch_name and branch.ip == self.ipAddress \
				and branch.port == self.portNumber):
				continue

			# Open a socket connection to the branch and store the socket in
			# branchPorts dictionary
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((branch.ip, int(branch.port)))
			self.branchPorts[branch.name] = sock
		
		# Loop and accept the TCP connection from all of the other branches
		i = 1
		while (i < len(new_branch_list)):
			# Accept a connection
			(clientSocket, address) = self.socket.accept()
			# Create another thread that will periodically send out transfers
			# to other branches
			try:
				listenerThread = ListenerThread(self, clientSocket)
				listenerThread.start()
			except Exception as e:
				print "(InitBranch) Could not create a new thread to listen for messages sent on this connection."
				print e
				sys.exit(1)
		
			i = i + 1

		# Create a new thread that will periodically send transfer messages to the
		# other branches.
		try: 
			transfersThread = TransfersThread(self)
			transfersThread.start()
		except Exception as e:
				print "(InitBranch) Could not create a new thread to listen for sending transfer messages to other branches."
				print e
				sys.exit(1)


	# This function will loop indefinitely. In each iteration of the loop, we'll
	# generate a random number between 0 and self.numOfMilliseconds, wait that amount
	# of time, then send a transfer of 1-5% of this branch's balance to another	
	# random branch. The only thread that will execute this function is the main
	# thread
	def sendTransfers(self):
		while True:
			# Make sure that there aren't any Marker messages being sent out
			if (self.sendingMarkers == True):
				continue

			# If branchPorts is an empty dictionary, then there are no other branches
			# in this distributed bank to send transfers to. So, just continue
			if (not self.branchPorts):
				print "(sendTransfers) Returning because there are no other" + \
						" branches in this distributed bank"	
				return

			# Generate random number in interval (0, numOfMilliseconds] and sleep for
			# that many milliseconds
			sleepTime = float(randint(0, self.numberOfMilliseconds))
			if (sleepTime > 0.0):
				sleepTime = sleepTime / 1000.0
				#print "(sendTransfers) Sleeping for", sleepTime, "seconds"
				time.sleep(sleepTime)
	
			# Choose a random branch
			randomBranch = choice(self.branchPorts.keys())

			# Creating the protobuf BranchMessage message to send the Transfer
			message = bank_pb2.BranchMessage()

			# ============== Critical Section Begin =========================
			threadLock.acquire(1)

			# Calculate 1-5% of this branch's balance
			amountToSend = (int) ( (float(randint(1,5)*self.balance)) / 100.0)
			self.balance = self.balance - amountToSend
		
			message.transfer.src_branch = self.branch_name
			message.transfer.dst_branch = randomBranch
			message.transfer.money = amountToSend

			# Send the Transfer message to the corresponding socket
			# TODO : After debugging, these two print statements should only occur
			# when self.numberOfMilliseconds >= 1000
			#if (self.numberOfMilliseconds >= 1000):
			print "(sendTransfers) Sending", amountToSend, "to " + randomBranch
			print "\tNew Balance of This Branch:", self.balance
			
			threadLock.release()
			# =============== Critical Section End ==========================

			sock = self.branchPorts[randomBranch]	
			sock.sendall(message.SerializeToString())

	def acceptMessages(self, clientSocket):

		# Create the BranchMessage
		receivedMessage = bank_pb2.BranchMessage()
		while True:
			# Receive a message from the clientSocket
			message = clientSocket.recv(4096)
			print "(acceptMessages) Received a message !"
			
			receivedMessage.ParseFromString(message)
			typeOfMessage = receivedMessage.WhichOneof("branch_message")
	
			if (typeOfMessage == "init_branch"):
				print "(acceptMessages) Receieved InitBranch message"
				self.InitBranch(receivedMessage.init_branch.balance, receivedMessage.init_branch.all_branches)

			elif (typeOfMessage == "transfer"):
				# ============== Outer Critical Section Begin ==================
				threadLock.acquire(1)

				# ============== Inner Critical Section Begin =================
				snapDictionaryLock.acquire(1)

				sourceBranch = receivedMessage.transfer.src_branch
				print "(acceptMessages) Receieved Transfer message"
				# TODO : After debugging, these two print statements should only occur
				# when self.numberOfMilliseconds >= 1000
				#if (self.numberOfMilliseconds >= 1000):
				print "(acceptMessages) Received", receivedMessage.transfer.money, \
						"from " + sourceBranch
				self.balance = self.balance + receivedMessage.transfer.money
				print "\tNew Balance of This Branch:", self.balance

				# Update channel states for all snapshot objects
				for key in self.snapshots:
					snapshotObj = self.snapshots[key]
					channel = snapshotObj.markerChannelState[sourceBranch]
					if (channel[1]):
						channel[0] = channel[0] + receivedMessage.transfer.money
						print "\tAdding", receivedMessage.transfer.money, "to " + \
							  "channel from " + sourceBranch + " to " + self.branch_name + \
							  " for snapshotID", snapshotObj.snapshotId
				# ============== Inner Critical Section End ===================
				snapDictionaryLock.release()

				threadLock.release()
				# =============== Outer Critical Section End =====================

			elif (typeOfMessage == "init_snapshot"):
				# ============= Critical Section Begin =====================
				snapDictionaryLock.acquire(1)

				print "(acceptMessages) Receieved InitSnapshot message"
				snapId = receivedMessage.init_snapshot.snapshot_id
				print "\tSnapshotId:", snapId
				newSnapObj = snapshot(self, snapId)
				
				self.snapshots[snapId] = newSnapObj
				newSnapObj.initSnapshot()

				snapDictionaryLock.release()
				# ============= Critical Section End =====================

			elif (typeOfMessage == "marker"):

				# ============= Critical Section Begin =====================
				snapDictionaryLock.acquire(1)
				print "(acceptMessages) Receieved Marker message"

				snapId = receivedMessage.marker.snapshot_id
				print "\tSnapshotId:", snapId
				otherBranch = receivedMessage.marker.src_branch

				# Received a marker message for a snapshot id that we're already
				# recording channels for this snapshot
				if (snapId in self.snapshots.keys()):
					self.snapshots[snapId].processSecondMarker()

				# Create new snapshot object for this snapshot
				else:
					newSnapObj = snapshot(self, snapId)
					self.snapshots[snapId] = newSnapObj
					newSnapObj.processFirstMarker(otherBranch)
				
				snapDictionaryLock.release()
				# ============= Critical Section End =====================

			elif (typeOfMessage == "retrieve_snapshot"):

				snapId = int(receivedMessage.retrieve_snapshot.snapshot_id)

				while True:
					# ============= Critical Section Begin =====================
					snapDictionaryLock.acquire(1)

					# If the requested snapshot hasn't been recorded by this branch,
					# then release the lock, sleep for a second, and continue this
					# loop
					if (not snapId in self.snapshots.keys()):
						snapDictionaryLock.release()
						time.sleep(1)
						continue
					# Otherwise, exit the loop
					else:
						break

				print "(acceptMessages) Receieved RetrieveSnapshot message"
				print "\tSnapshotId:", snapId
				
				snapObject = self.snapshots[snapId]
				
				returnSnapMessage = bank_pb2.BranchMessage()
				returnSnapMessage.return_snapshot.local_snapshot.snapshot_id = snapId
				returnSnapMessage.return_snapshot.local_snapshot.balance = \
							snapObject.markerBalance
			
				channelStates = []

				snapDictionaryLock.release()
				# ============= Critical Section End =====================

				# Get the states of all of the channels. Need this while loop because we
				# must release the lock and try again
				while True:
					# ================ Critical Section Begin =================
					snapDictionaryLock.acquire(1)
					for branchName in sorted(snapObject.markerChannelState.keys()):
						channelStateArray = snapObject.markerChannelState[branchName]
						# If channel is closed, then record the channel state
						if (channelStateArray[1] == False):
							channelStates.append(channelStateArray[0])
						# Otherwise, release the lock, reset channelStates, sleep for a second,
						# and break this loop
						else:
							print "\t\tReleasing the lock and retrying because " + branchName + "'s" +\
									" channel is still open"
							snapDictionaryLock.release()
							channelStates = []
							time.sleep(1)
							break

					# If channelStates is filled (i.e., not = []), then we can break out
					# of this while loop
					if (channelStates != []):
						break

				returnSnapMessage.return_snapshot.local_snapshot.channel_state.extend(channelStates)
				clientSocket.sendall(returnSnapMessage.SerializeToString())
				
				snapDictionaryLock.release()
				# ============= Critical Section End =====================

			elif (typeOfMessage == "return_snapshot"):
				print "(acceptMessages) Receieved ReturnSnapshot message"

			else:
				assert typeOfMessage is None
				print "(acceptMessages) Error: Receieved message has unknown type"
				sys.exit(1)

			# Clear the BranchMessage (receivedMessage)
			receivedMessage.Clear()

# Main routine for branch
def main():
	numberOfArgs = len(sys.argv)

	if (numberOfArgs != 4):
		print "Usage: ./Branch.py <Branch-Name> <Port-Number> <Number-Milliseconds>"
		sys.exit(1)

	branchName = sys.argv[1]
	portNumber = int(sys.argv[2])
	numberOfMilliseconds = int(sys.argv[3])

	branch = bank(branchName, portNumber, numberOfMilliseconds)

	try:
		# Accept a connection from Controller process
		(clientSocket, address) = branch.socket.accept()

		# Will enter an indefinite loop that waits for a Controller process to 
		# send an InitBranch message
		branch.acceptMessages(clientSocket)

	except KeyboardInterrupt:
		print "Received Keyboard Interrupt"
		sys.exit(0)

main()
