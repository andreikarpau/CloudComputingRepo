/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
//	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();
	Node self(this->memberNode->addr);
	curMemList.push_back(self);

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	int nodeIndex = 0;

	Node secPredecessor;
	Node predecessor;
	Node successor;
	Node secSuccessor;

	for (unsigned int i = 0; i < curMemList.size(); i++) {

		if (self.nodeHashCode == curMemList[i].nodeHashCode){
			nodeIndex = i;
			break;
		}
	}

	if (nodeIndex == 0)
	{
		predecessor = curMemList[curMemList.size() - 1];
		secPredecessor = curMemList[curMemList.size() - 2];
	}
	else if (nodeIndex == 1)
	{
		predecessor = curMemList[nodeIndex - 1];
		secPredecessor = curMemList[curMemList.size() - 1];
	}
	else
	{
		predecessor = curMemList[nodeIndex - 1];
		secPredecessor = curMemList[nodeIndex - 2];
	}

	if (((int)curMemList.size() - 1) == nodeIndex)
	{
		successor = curMemList[0];
		secSuccessor = curMemList[1];
	}
	else if (((int)curMemList.size() - 2) == nodeIndex)
	{
		successor = curMemList[nodeIndex + 1];
		secSuccessor = curMemList[0];
	}
	else
	{
		successor = curMemList[nodeIndex + 1];
		secSuccessor = curMemList[nodeIndex + 2];
	}

	ring = curMemList;
	stabilizationProtocol(secPredecessor, predecessor, successor, secSuccessor);
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

char MP2Node::GetReplicaNum(ReplicaType replica)
{
	switch (replica)
	{
		case PRIMARY:
			return 0;
		break;
		case SECONDARY:
			return 1;
		break;
		case TERTIARY:
			return 2;
		break;
	}

	return 0;
}

ReplicaType MP2Node::GetReplicaByNum(char replicaNum)
{
	switch (replicaNum)
	{
		case 0:
			return PRIMARY;
		break;
		case 1:
			return SECONDARY;
		break;
		case 2:
			return TERTIARY;
		break;
	}

	return PRIMARY;
}

void MP2Node::SendMessage(string message, Address* toAddr){
	size_t msgsize = sizeof(MessageHdr) + message.size() + 1;
	MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));

	msg->msgType = KEYVALMSG;
	msg->senderHeartbeat = memberNode->heartbeat;
	memcpy((char*) (msg + 1), message.c_str(), message.size() * sizeof(char));

	emulNet->ENsend(&memberNode->addr, toAddr, (char *) msg, msgsize);
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */

	vector<Node> nodes = findNodes(key);

	g_transID++;
	Message message(g_transID, this->memberNode->addr, CREATE, key, value, PRIMARY);
	SendMessage(message.toString(), &nodes[0].nodeAddress);

	Message messageS(g_transID, this->memberNode->addr, CREATE, key, value, SECONDARY);
	SendMessage(messageS.toString(), &nodes[1].nodeAddress);

	Message messageT(g_transID, this->memberNode->addr, CREATE, key, value, TERTIARY);
	SendMessage(messageT.toString(), &nodes[2].nodeAddress);

	SendMessagesBatch batch;
	batch.successNum = 0;
	batch.liveTime = 0;
	batch.key = key;
	batch.value = value;
	batch.type = CREATE;
	batch.transID = g_transID;
	sentMessages.push_back(batch);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
	vector<Node> nodes = findNodes(key);

	g_transID++;
	Message message(g_transID, this->memberNode->addr, READ, key);
	SendMessage(message.toString(), &nodes[0].nodeAddress);

	Message messageS(g_transID, this->memberNode->addr, READ, key);
	SendMessage(messageS.toString(), &nodes[1].nodeAddress);

	Message messageT(g_transID, this->memberNode->addr, READ, key);
	SendMessage(messageT.toString(), &nodes[2].nodeAddress);

	ReadMessagesBatch batch;
	batch.liveTime = 0;
	batch.key = key;
	batch.values[0] = "";
	batch.values[1] = "";
	batch.values[1] = "";
	batch.resultValue = "";
	batch.transID = g_transID;
	readMessages.push_back(batch);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
	vector<Node> nodes = findNodes(key);

	g_transID++;
	Message message(g_transID, this->memberNode->addr, UPDATE, key, value);
	SendMessage(message.toString(), &nodes[0].nodeAddress);

	Message messageS(g_transID, this->memberNode->addr, UPDATE, key, value);
	SendMessage(messageS.toString(), &nodes[1].nodeAddress);

	Message messageT(g_transID, this->memberNode->addr, UPDATE, key, value);
	SendMessage(messageT.toString(), &nodes[2].nodeAddress);

	SendMessagesBatch batch;
	batch.successNum = 0;
	batch.liveTime = 0;
	batch.key = key;
	batch.value = value;
	batch.type = UPDATE;
	batch.transID = g_transID;
	sentMessages.push_back(batch);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	vector<Node> nodes = findNodes(key);

	g_transID++;
	Message message(g_transID, this->memberNode->addr, DELETE, key);
	SendMessage(message.toString(), &nodes[0].nodeAddress);

	Message messageS(g_transID, this->memberNode->addr, DELETE, key);
	SendMessage(messageS.toString(), &nodes[1].nodeAddress);

	Message messageT(g_transID, this->memberNode->addr, DELETE, key);
	SendMessage(messageT.toString(), &nodes[2].nodeAddress);

	SendMessagesBatch batch;
	batch.successNum = 0;
	batch.liveTime = 0;
	batch.key = key;
	batch.type = DELETE;
	batch.transID = g_transID;
	sentMessages.push_back(batch);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */

	if (0 < ht->count(key))
	{
		Entry entry(ht->read(key));
		entry.replica = replica;

		for (unsigned int i = 0; i < savedInfos.size(); i++){
			if (savedInfos[i].key == key){
				savedInfos[i].replica = replica;
			}
		}

		return true;
	}


	// Insert key, value, replicaType into the hash table
	switch (replica)
	{
		case PRIMARY:
			break;
		case SECONDARY:
			break;
		case TERTIARY:
			break;
	}

	Entry entry(value, 1, replica);
	ht->create(key, entry.convertToString());

	SavedInfo info;
	info.key = key;
	info.replica = replica;
	savedInfos.push_back(info);

	return true;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	try
	{
		if (ht->count(key) <= 0)
			return "";

		Entry entry(ht->read(key));
		return entry.value;
	}
	catch (int e)
	{
		return "";
	}
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */

	// Update key in local hash table and return true or false
	try
	{
		if (ht->count(key) <= 0)
			return false;

		Entry entry(ht->read(key));
		entry.timestamp++;
		entry.value = value;
		ht->update(key, entry.convertToString());
	}
	catch (int e)
	{
		return false;
	}

	return true;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	if (ht->count(key) <= 0)
		return false;

	ht->deleteKey(key);

	for (unsigned int i = 0; i < savedInfos.size(); i++){
		if (savedInfos[i].key == key){
			savedInfos.erase(savedInfos.begin() + i);
			i--;
		}
	}

	return true;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
		MessageHdr* getMsg = (MessageHdr*) data;
		MsgTypes type = getMsg->msgType;
		if (type != KEYVALMSG)
			return;

		int newMessageSize = size - sizeof(MessageHdr) - 1;
		char* newMessage = new char[newMessageSize];
		memcpy(newMessage, (char*) (getMsg + 1), newMessageSize);

		string messageStr((const char*)newMessage, 0, newMessageSize);
		Message messageRes(messageStr);
		bool success = true;
		string readResult = "";

		Message replMessage(messageRes.transID, this->memberNode->addr, REPLY, true);

		switch (messageRes.type)
		{
			case CREATE:
				success = createKeyValue(messageRes.key, messageRes.value, messageRes.replica);

				if (success)
					log->logCreateSuccess(&this->memberNode->addr, false, messageRes.transID, messageRes.key, messageRes.value);
				else
					log->logCreateFail(&this->memberNode->addr, false, messageRes.transID, messageRes.key, messageRes.value);

				replMessage.success = success;
				SendMessage(replMessage.toString(), &messageRes.fromAddr);
				break;
			case UPDATE:
				success = updateKeyValue(messageRes.key, messageRes.value, messageRes.replica);

				if (success)
					log->logUpdateSuccess(&this->memberNode->addr, false, messageRes.transID, messageRes.key, messageRes.value);
				else
					log->logUpdateFail(&this->memberNode->addr, false, messageRes.transID, messageRes.key, messageRes.value);

				replMessage.success = success;
				SendMessage(replMessage.toString(), &messageRes.fromAddr);
				break;
			case DELETE:
				success = deletekey(messageRes.key);

				if (success)
					log->logDeleteSuccess(&this->memberNode->addr, false, messageRes.transID, messageRes.key);
				else
					log->logDeleteFail(&this->memberNode->addr, false, messageRes.transID, messageRes.key);

				replMessage.success = success;
				SendMessage(replMessage.toString(), &messageRes.fromAddr);
				break;
			case READ:
				readResult = readKey(messageRes.key);
				success = readResult != "";

				if (success)
					log->logReadSuccess(&this->memberNode->addr, false, messageRes.transID, messageRes.key, readResult);
				else
					log->logReadFail(&this->memberNode->addr, false, messageRes.transID, messageRes.key);

				replMessage.type = READREPLY;
				replMessage.value = readResult;
				SendMessage(replMessage.toString(), &messageRes.fromAddr);
				break;
			case REPLY:
				for (unsigned int i = 0; i < sentMessages.size(); i++){
					if (sentMessages[i].transID == messageRes.transID && messageRes.success){
						sentMessages[i].successNum++;
						break;
					}
				}
				break;
			case READREPLY:
				for (unsigned int i = 0; i < readMessages.size(); i++){
					if (readMessages[i].transID == messageRes.transID){
						if (readMessages[i].resultValue == "" && messageRes.value != ""){
							for (int j = 0; j < 3; j++){
								if (readMessages[i].values[j] == ""){
									readMessages[i].values[j] = messageRes.value;
									break;
								}
								else if (readMessages[i].values[j] == messageRes.value){
									readMessages[i].resultValue = messageRes.value;
									break;
								}
							}

						}
						break;
					}
				}

				break;
		}
	}

	for (unsigned int i = 0; i < readMessages.size(); i++){
		readMessages[i].liveTime++;
		if (readMessages[i].liveTime < sentMessagesBatchLiveTime)
			continue;

		if (readMessages[i].resultValue != ""){
			log->logReadSuccess(&this->memberNode->addr, true, readMessages[i].transID, readMessages[i].key, readMessages[i].resultValue);
		}
		else{
			log->logReadFail(&this->memberNode->addr, true, readMessages[i].transID, readMessages[i].key);
		}

		readMessages.erase(readMessages.begin() + i);
		i--;
	}

	for (unsigned int i = 0; i < sentMessages.size(); i++){
		sentMessages[i].liveTime++;
		if (sentMessages[i].liveTime < sentMessagesBatchLiveTime)
			continue;

		if (2 <= sentMessages[i].successNum){
			switch (sentMessages[i].type){
				case CREATE:
					log->logCreateSuccess(&this->memberNode->addr, true, sentMessages[i].transID, sentMessages[i].key, sentMessages[i].value);
					break;
				case UPDATE:
					log->logUpdateSuccess(&this->memberNode->addr, true, sentMessages[i].transID, sentMessages[i].key, sentMessages[i].value);
					break;
				case DELETE:
					log->logDeleteSuccess(&this->memberNode->addr, true, sentMessages[i].transID, sentMessages[i].key);
					break;
				default:
					break;
			}
		}
		else {
			switch (sentMessages[i].type){
				case CREATE:
					log->logCreateFail(&this->memberNode->addr, true, sentMessages[i].transID, sentMessages[i].key, sentMessages[i].value);
					break;
				case UPDATE:
					log->logUpdateFail(&this->memberNode->addr, true, sentMessages[i].transID, sentMessages[i].key, sentMessages[i].value);
					break;
				case DELETE:
					log->logDeleteFail(&this->memberNode->addr, true, sentMessages[i].transID, sentMessages[i].key);
					break;
				default:
					break;
			}
		}

		sentMessages.erase(sentMessages.begin() + i);
		i--;
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (unsigned int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

void MP2Node::SendStabilizationMessage(string key, ReplicaType replica, Address* sendTo){
	Entry entry(ht->read(key));

	g_transID++;
	Message message(g_transID, this->memberNode->addr, CREATE, key, entry.value, replica);
	SendMessage(message.toString(), sendTo);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol(Node secPredecessor, Node predecessor, Node successor, Node secSuccessor) {
	if (haveReplicasOf.size() == 2 && secPredecessor.nodeAddress.getAddress() != haveReplicasOf[0].nodeAddress.getAddress()){
		for (unsigned int i = 0; i < savedInfos.size(); i++){
			if (savedInfos[i].replica == TERTIARY){
				SendStabilizationMessage(savedInfos[i].key, PRIMARY, &secPredecessor.nodeAddress);
			}
		}
	}

	if (haveReplicasOf.size() == 2 && predecessor.nodeAddress.getAddress() != haveReplicasOf[1].nodeAddress.getAddress()){
		for (unsigned int i = 0; i < savedInfos.size(); i++){
			if (savedInfos[i].replica == TERTIARY){
				SendStabilizationMessage(savedInfos[i].key, SECONDARY, &predecessor.nodeAddress);
			}
			else if (savedInfos[i].replica == SECONDARY){
				SendStabilizationMessage(savedInfos[i].key, PRIMARY, &predecessor.nodeAddress);
			}
		}
	}

	haveReplicasOf.clear();
	haveReplicasOf.push_back(secPredecessor);
	haveReplicasOf.push_back(predecessor);

	if (hasMyReplicas.size() == 2 && successor.nodeAddress.getAddress() != hasMyReplicas[0].nodeAddress.getAddress()){
		for (unsigned int i = 0; i < savedInfos.size(); i++){
			if (savedInfos[i].replica == PRIMARY){
				SendStabilizationMessage(savedInfos[i].key, SECONDARY, &successor.nodeAddress);
			}
			else if (savedInfos[i].replica == SECONDARY){
				SendStabilizationMessage(savedInfos[i].key, TERTIARY, &successor.nodeAddress);
			}
		}
	}

	if (hasMyReplicas.size() == 2 && secSuccessor.nodeAddress.getAddress() != hasMyReplicas[1].nodeAddress.getAddress()){
		for (unsigned int i = 0; i < savedInfos.size(); i++){
			if (savedInfos[i].replica == PRIMARY){
				SendStabilizationMessage(savedInfos[i].key, TERTIARY, &secSuccessor.nodeAddress);
			}
		}
	}

	hasMyReplicas.clear();
	hasMyReplicas.push_back(successor);
	hasMyReplicas.push_back(secSuccessor);
}
