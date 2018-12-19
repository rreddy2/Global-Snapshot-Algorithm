import os
import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import bank_pb2
import socket
import time
from random import randint
class snapshot:
	'''
		PLAN TO MARK CHANNEL STATES
			Can set a tuple as the key (snapshotID, SendingBranch) and the value can be the amount sent
				We would update this every time there is a transfer sent out

		To initialize it after a branch receives the first marker:
			markerChannelState[(snapshotID,receivingBranchName)] = (0, True)
		To close the channel after receiving another marker message:
			markerChannelState[(snapshotID, receivingBranchName)] = (<amount transfered>, False)

		Need to figure out how to get that <amount transfered> number too

	'''

	# Pass the branch that is calling the snapshot algorithm
	def __init__(self, new_bank, snapId):
		self.bank = new_bank
		self.snapshotId = snapId
		self.markerBalance = 0

		# Dictionary where keys are the branch name of the other side of the channel
		# (String) and the values are arrays [ amount (int), recordingChannel (bool) ]
		self.markerChannelState = {}

	# Save the initial state and send marker
	# messages to all other branches
	def initSnapshot(self):
		#record initial state in the dict
		self.markerBalance = self.bank.getBalance()	
		#send marker messages to all other branches	
		self.sendMarkers()

	#creates a Marker Message object and sends it to
	#all other branches
	def sendMarkers(self):

		# Set self.bank.sendingMarkers to True so that no Transfers are sent out
		# while the markers are being sent out
		self.bank.sendingMarkers = True

		# Sleep for the max amount of time between Transfer messages (and even a
		# little longer) to make sure that the thread sending Transfer messages has
		# enough time to discover that self.bank.sendingMarkers == True
		sleepTime = float(randint(0, self.bank.numberOfMilliseconds))
		sleepTime = sleepTime + .5
		time.sleep(sleepTime)

		for b in self.bank.branches:
			self.markerChannelState[b.name] = [0, True]

			#Check to not send the message to itself (branch names are unique)
			if (self.bank.branch_name == b.name):
				continue
			#creates the BranchMessage() object 
			markerMessage = bank_pb2.BranchMessage()
			#populates the markerMessage object
			markerMessage.marker.snapshot_id = int(self.snapshotId)
			markerMessage.marker.src_branch = self.bank.branch_name
			markerMessage.marker.dst_branch = b.name
			try:
				sock = self.bank.branchPorts[str(b.name)]
			except KeyError as e:
				print e
				print "b.name: " + b.name
				print self.bank.branchPorts.keys()
				sys.exit(1)
			sock.sendall(markerMessage.SerializeToString())

		# Set self.bank.sendingMarkers to False so that Transfers can now be sent out
		self.bank.sendingMarkers = False

	def processSecondMarker(self):
		for key in self.markerChannelState:
			self.markerChannelState[key][1] = False

	def processFirstMarker(self, receivingBranchName):
		self.markerBalance = self.bank.getBalance()
		#Send marker messages out to all other branches
		self.sendMarkers()

