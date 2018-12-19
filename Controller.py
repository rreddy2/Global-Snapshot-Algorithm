#!/usr/bin/env python

from __future__ import print_function
import os
import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import bank_pb2
import socket
import time 					# Used for sleep()
from random import choice		# Used for generating random integers
import snapshot

# Main routine for the controller
def main():
	numberOfArgs = len(sys.argv)
	
	if (numberOfArgs != 3):
		print("Usage: ./Controller.py <Total-Balance> <branches.txt>")
		sys.exit(1)

	totalBalance = int(sys.argv[1])
	inputFileName = sys.argv[2]

	print("Total Balance: " + str(totalBalance))
	print("Input File Name: " + inputFileName)
	print("")

	# Open branches.txt and record the branch name, ip address, and port number
	# for each branch that is running
	branches = []
	inputFile = open(inputFileName, "r")
	for line in inputFile:
		branchInfo = line.split()
		branches.append(branchInfo)

	# Iterate through branches array and print output for debugging
	#`for branch in branches:
	#	print "Branch Name: " + branch[0]
	#	print "IP Address: " + branch[1]
	#	print "Port Number: " + branch[2]
	#	print ""

	initialBalance = totalBalance // len(branches)
	print("Each branch gets " + str(initialBalance) + " as initial balance\n")
	
	allBranches = []
	for branch in branches:
		someBranch = bank_pb2.InitBranch.Branch()
		someBranch.name = branch[0]
		someBranch.ip = branch[1]
		someBranch.port = int(branch[2])
		allBranches.append(someBranch)

	# Dictionary where keys are branch names (String) and values are open
	# socket objects to that branch
	branchPorts = {}

	# Iterate through branches array and send InitBranch message to each branch
	for branch in branches:
		# Creating the protobuf BranchMessage message to be sent to each branch
		message = bank_pb2.BranchMessage()

		message.init_branch.balance = initialBalance
		message.init_branch.all_branches.extend(allBranches)
	
		# Open a socket connection to the branch and send the message
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((branch[1], int(branch[2])))
		sock.sendall(message.SerializeToString())

		# Store the socket connection to this branch in branchPorts dictionary
		branchPorts[branch[0]] = sock



	# Periodically send an InitSnapshot message to a random branch, wait for the Snapshot algorithm 
	# to complete, then send RetrieveSnapshot messages and print resulting global snapshot
	initSnapMessage = bank_pb2.BranchMessage()
	snapshotId = 1
	
	time.sleep(5)

	while True:
		# Choose a random branch
		randomBranch = choice(branchPorts.keys())
		
		# Send InitSnapshot message to the random branch
		initSnapMessage.init_snapshot.snapshot_id = snapshotId
		sock = branchPorts[randomBranch]
		sock.sendall(initSnapMessage.SerializeToString())

		# Sleep for 5 seconds
		time.sleep(10)

		print("Snapshot_id: " + str(snapshotId))
		
		returnSnapMessage = bank_pb2.BranchMessage()
		for branchName in branchPorts:
			sock = branchPorts[branchName]
			getSnapMessage = bank_pb2.BranchMessage()
			getSnapMessage.retrieve_snapshot.snapshot_id = snapshotId
			sock.sendall(getSnapMessage.SerializeToString())

			returnSnapMessage.ParseFromString(sock.recv(4096))
			branchState = returnSnapMessage.return_snapshot.local_snapshot.balance
			channelStates = returnSnapMessage.return_snapshot.local_snapshot.channel_state
			print(branchName + ": " +  str(branchState) + ", ", end='')
			i = 0
			for otherBranchName in sorted(branchPorts.keys()):
				if otherBranchName == branchName:
					continue
				print(otherBranchName + "->" + branchName + ": " + \
						str(channelStates[i]) + ", ", end='')
				i = i + 1
			print("")
			returnSnapMessage.Clear()

		snapshotId = snapshotId + 1
		print("")
		initSnapMessage.Clear()

main()
