/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log,
		Address *address) {
	for (int i = 0; i < 6; i++) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
	if (memberNode->bFailed) {
		return false;
	} else {
		return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1,
				&(memberNode->mp1q));
	}
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *) env, (void *) buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
	Address joinaddr;
	joinaddr = getJoinAddress();

	// Self booting routines
	if (initThisNode(&joinaddr) == -1) {
#ifdef DEBUGLOG
		log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
		exit(1);
	}

	if (!introduceSelfToGroup(&joinaddr)) {
		finishUpThisNode();
#ifdef DEBUGLOG
		log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
		exit(1);
	}

	return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*) (&memberNode->addr.addr);
	int port = *(short*) (&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
	// node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
	initMemberListTable(memberNode);

	return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
	static char s[1024];
#endif

	if (0
			== memcmp((char *) &(memberNode->addr.addr),
					(char *) &(joinaddr->addr),
					sizeof(memberNode->addr.addr))) {
		// I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
		log->LOG(&memberNode->addr, "Starting up group...");
#endif
		memberNode->inGroup = true;
	} else {
		size_t msgsize = sizeof(MessageHdr) + sizeof(&memberNode->addr.addr)
				+ 1;
		msg = (MessageHdr *) malloc(msgsize * sizeof(char));

		// create JOINREQ message: format of data is {struct Address myaddr}
		msg->msgType = JOINREQ;
		msg->senderHeartbeat = memberNode->heartbeat;
		memcpy((char*) (msg + 1), &memberNode->addr.addr,
				sizeof(memberNode->addr.addr));

#ifdef DEBUGLOG
		sprintf(s, "Trying to join...");
		log->LOG(&memberNode->addr, s);
#endif

		// send JOINREQ message to introducer member
		emulNet->ENsend(&memberNode->addr, joinaddr, (char *) msg, msgsize);

		free(msg);
	}

	return 1;

}

void MP1Node::sendListMsg(MsgTypes headerType, Address* sendAddress) {
	MessageHdr* msg;

	int listCount = memberNode->memberList.size();
	vector<ItemInfo> array;

	for (int i = 0; i < listCount; i++) {
		MemberListEntry listEntry = memberNode->memberList[i];

		// failed item then do not send
		if (listEntry.timestamp < 0)
			continue;

		ItemInfo info = ItemInfo();
		info.heartbeat = listEntry.heartbeat;
		info.id = listEntry.id;
		info.port = listEntry.port;
		array.push_back(info);
	}

	listCount = array.size();
	int entrySize = sizeof(ItemInfo);

	int sizeArray = listCount * entrySize;
	int sizeAddress = sizeof(&memberNode->addr.addr);

	size_t msgsize = sizeof(MessageHdr) + sizeArray + 1 + sizeAddress;

	msg = (MessageHdr *) malloc(msgsize * sizeof(char));
	msg->msgType = headerType;
	msg->senderHeartbeat = memberNode->heartbeat;
	memcpy((char*) (msg + 1), &memberNode->addr.addr, sizeAddress);

	if (0 < listCount) {

		memcpy((char*) (msg + 1) + 1 + sizeAddress, &array[0], sizeArray);
	}

	// send JOINREQ message to introducer member
	emulNet->ENsend(&(memberNode->addr), sendAddress, (char *) msg, msgsize);
	free(msg);
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode() {
	/*
	 * Your code goes here
	 */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
	if (memberNode->bFailed) {
		return;
	}

	// Check my messages
	checkMessages();

	// Wait until you're in the group...
	if (!memberNode->inGroup) {
		return;
	}

	// ...then jump in and share your responsibilites!
	nodeLoopOps();

	return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
	void *ptr;
	int size;

	// Pop waiting messages from memberNode's mp1q
	while (!memberNode->mp1q.empty()) {
		ptr = memberNode->mp1q.front().elt;
		size = memberNode->mp1q.front().size;
		memberNode->mp1q.pop();
		recvCallBack((void *) memberNode, (char *) ptr, size);
	}
	return;
}

void MP1Node::addOrEditListEntry(char* fromAddress, long heartBeat) {
	int id = *(int*) (&fromAddress[0]);
	int port = *(short*) (&fromAddress[4]);
	addOrEditListEntry(id, port, heartBeat);
}

void MP1Node::addOrEditListEntry(int id, int port, long heartBeat) {
	Address* newAddress = GetAddressFromStr(id, port);

	if (*newAddress == memberNode->addr) {
		free(newAddress);
		newAddress = NULL;
		return;
	}

	for (uint i = 0; i < memberNode->memberList.size(); i++) {
		MemberListEntry* listEntry = &memberNode->memberList[i];
		if (listEntry->id == id && listEntry->port == port) {
			if (listEntry->heartbeat < heartBeat) {
				listEntry->setheartbeat(heartBeat);
				listEntry->settimestamp(memberNode->heartbeat);
			}

			free(newAddress);
			newAddress = NULL;
			return;
		}
	}

	memberNode->memberList.push_back(
			MemberListEntry(id, port, heartBeat, memberNode->heartbeat));
	log->logNodeAdd(&memberNode->addr, newAddress);
	free(newAddress);
	newAddress = NULL;
}

Address* MP1Node::GetAddressFromStr(char *data) {
	int id = 0;
	short port;
	memcpy(&id, &data[0], sizeof(int));
	memcpy(&port, &data[4], sizeof(short));
	return GetAddressFromStr(id, port);
}

Address* MP1Node::GetAddressFromStr(int id, short port) {
	return new Address(to_string(id) + ":" + to_string(port));
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
	/*
	 * Your code goes here
	 */

	MessageHdr* msg;
	msg = (MessageHdr*) data;

	MsgTypes type = msg->msgType;
	long senderHeartbeat = msg->senderHeartbeat;
	int addressSize = sizeof(&memberNode->addr.addr);
	char* fromAddress;
	fromAddress = (char*) malloc(addressSize);
	memcpy(fromAddress, (char*) (msg + 1), addressSize);

	switch (type) {

	case JOINREP:
		memberNode->inGroup = true;
		/* no break */
	case GOSSIP:
		int entrySize;
		entrySize = sizeof(ItemInfo);
		int hdrSize;
		hdrSize = sizeof(MessageHdr);
		int sizeArray;
		sizeArray = (size - hdrSize - sizeof(&memberNode->addr.addr));
		int listCount;
		listCount = sizeArray / entrySize;

		if (0 < listCount) {
			ItemInfo* list;
			list = (ItemInfo*) malloc(sizeArray);
			memcpy(list, (char*) (msg + 1) + 1 + addressSize, sizeArray);

			for (int i = 0; i < listCount; i++) {
				ItemInfo listEntry;
				listEntry = list[i];
				addOrEditListEntry(listEntry.id, listEntry.port,
						listEntry.heartbeat);
			}

			free(list);
		}

		addOrEditListEntry(fromAddress, senderHeartbeat);
		break;

	case JOINREQ:
		addOrEditListEntry(fromAddress, senderHeartbeat);
		Address* toAddress;
		toAddress = GetAddressFromStr(fromAddress);
		sendListMsg(JOINREP, toAddress);
		break;

	default:
		break;
	}

	free(fromAddress);
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
	/*
	 * Your code goes here
	 */
	memberNode->heartbeat++;

	// delete failed items
	int listCount = memberNode->memberList.size();
	for (int i = 0; i < listCount; i++) {
		MemberListEntry listEntry = memberNode->memberList[i];

		if (0 <= listEntry.timestamp
				&& TFAIL < (memberNode->heartbeat - listEntry.timestamp)) {
			memberNode->memberList[i].settimestamp(-TREMOVE);
			continue;
		}

		if (listEntry.timestamp == -1) {
			memberNode->memberList.erase(memberNode->memberList.begin() + i);
			i--;
			listCount--;
			log->logNodeRemove(&memberNode->addr,
					GetAddressFromStr(listEntry.id, listEntry.port));
			continue;
		}

		if (listEntry.timestamp < -1) {
			memberNode->memberList[i].settimestamp(listEntry.timestamp + 1);
		}
	}

	if (memberNode->memberList.size() <= 0) {
		return;
	}

	srand(time(NULL));

	vector<int> sentItems;
	listCount = memberNode->memberList.size();

	for (int i = 0; i < maxGossipSends; i++) {
		int elementNumber = rand() % listCount;

		if (i == (maxGossipSends - 1)) {
			elementNumber = extraSendIndex;
			extraSendIndex++;
			if (extraSendIndex == listCount){
				extraSendIndex = 0;
			}
		}

		if (listCount <= elementNumber) {
			continue;
		}

		if (i > 0) {
			if (find(sentItems.begin(), sentItems.end(), elementNumber)
					!= sentItems.end()) {
				continue;
			}
		}

		MemberListEntry listEntry = memberNode->memberList[elementNumber];
		sentItems.push_back(elementNumber);

		Address* toAddress;
		toAddress = GetAddressFromStr(listEntry.id, listEntry.port);
		sendListMsg(GOSSIP, toAddress);
	}
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
	Address joinaddr;

	memset(&joinaddr, 0, sizeof(Address));
	*(int *) (&joinaddr.addr) = 1;
	*(short *) (&joinaddr.addr[4]) = 0;

	return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr) {
	printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
			addr->addr[3], *(short*) &addr->addr[4]);
}
