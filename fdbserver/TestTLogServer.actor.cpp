/*
 * MockTLog.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbserver/TestTLogServer.actor.h"

#include <filesystem>
#include <vector>

#include "fdbrpc/Locality.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/IDiskQueue.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/LogSystem.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // has to be last include

// Unit tests that start TLogs and drive transactions through them. There is one test that calls
// the TLog commit interface directly and another using the LogSystem's push interface.
// The later supports a single log group made up of "numLogServers" servers and
// "numTagsPerServer" tags per log server. A third test exercises tLog recovery.
// These test's purpose is to help with microbenchmarks and experimentation.

// build test state.
std::shared_ptr<TLogTestContext> initTLogTestContext(TestTLogOptions tLogOptions, int locality = 0) {
	std::shared_ptr<TLogTestContext> context(new TLogTestContext(tLogOptions));
	context->logID = deterministicRandom()->randomUniqueID();
	context->workerID = deterministicRandom()->randomUniqueID();
	context->diskQueueBasename = tLogOptions.diskQueueBasename;
	context->numCommits = tLogOptions.numCommits;
	context->numTagsPerServer = tLogOptions.numTagsPerServer;
	context->numLogServers = tLogOptions.numLogServers;
	context->dcID = StringRef("test");
	context->tagLocality = locality; // one data center.
	context->dbInfo = ServerDBInfo();
	context->dbInfoRef = makeReference<AsyncVar<ServerDBInfo>>(context->dbInfo);
	context->dbInfo.logSystemConfig.logSystemType = LogSystemType::tagPartitioned;
	context->dbInfo.logSystemConfig.recruitmentID = deterministicRandom()->randomUniqueID();

	return context;
}

// run a single tLog. If optional parmeters are set, the tLog is a new generation of "tLogID"
// as described in initReq. Otherwise, it is a generation 0 tLog.
ACTOR Future<Void> getTLogCreateActor(std::shared_ptr<TLogTestContext> pTLogTestContext,
                                      TestTLogOptions tLogOptions,
                                      uint16_t processID,
                                      InitializeTLogRequest* initReq = nullptr,
                                      UID tLogID = UID()) {

	// build per-tLog state.
	state std::shared_ptr<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[processID];
	pTLogContext->tagProcessID = processID;

	pTLogContext->tLogID = tLogID != UID(0, 0) ? tLogID : deterministicRandom()->randomUniqueID();

	TraceEvent("TestTLogServerEnterGetTLogCreateActor", pTLogContext->tLogID).detail("Epoch", pTLogTestContext->epoch);

	std::filesystem::create_directory(tLogOptions.dataFolder);

	// create persistent storage
	std::string diskQueueBasename = pTLogTestContext->diskQueueBasename + "." + pTLogContext->tLogID.toString() + "." +
	                                std::to_string(pTLogTestContext->epoch) + ".";
	state std::string diskQueueFilename = tLogOptions.dataFolder + "/" + diskQueueBasename;
	pTLogContext->persistentQueue =
	    openDiskQueue(diskQueueFilename, tLogOptions.diskQueueExtension, pTLogContext->tLogID, DiskQueueVersion::V1);

	state std::string kvStoreFilename = tLogOptions.dataFolder + "/" + tLogOptions.kvStoreFilename + "." +
	                                    pTLogContext->tLogID.toString() + "." +
	                                    std::to_string(pTLogTestContext->epoch) + ".";
	pTLogContext->persistentData = keyValueStoreMemory(kvStoreFilename,
	                                                   pTLogContext->tLogID,
	                                                   tLogOptions.kvMemoryLimit,
	                                                   tLogOptions.kvStoreExtension,
	                                                   KeyValueStoreType::MEMORY_RADIXTREE);

	// prepare tLog construction.
	Standalone<StringRef> machineID = StringRef("machine");
	LocalityData localities(
	    Optional<Standalone<StringRef>>(), pTLogTestContext->zoneID, machineID, pTLogTestContext->dcID);
	localities.set(StringRef("datacenter"), pTLogTestContext->dcID);
	pTLogTestContext->dbInfoRef = makeReference<AsyncVar<ServerDBInfo>>(pTLogTestContext->dbInfo);

	Reference<AsyncVar<bool>> isDegraded = FlowTransport::transport().getDegraded();
	Reference<AsyncVar<UID>> activeSharedTLog(new AsyncVar<UID>(pTLogContext->tLogID));
	Reference<AsyncVar<bool>> enablePrimaryTxnSystemHealthCheck(new AsyncVar<bool>(false));
	state PromiseStream<InitializeTLogRequest> promiseStream = PromiseStream<InitializeTLogRequest>();
	Promise<Void> oldLog;
	Promise<Void> recovery;

	// construct tLog.
	state Future<Void> tl = ::tLog(pTLogContext->persistentData,
	                               pTLogContext->persistentQueue,
	                               pTLogTestContext->dbInfoRef,
	                               localities,
	                               promiseStream,
	                               pTLogContext->tLogID,
	                               pTLogTestContext->workerID,
	                               false, /* restoreFromDisk */
	                               oldLog,
	                               recovery,
	                               pTLogTestContext->diskQueueBasename,
	                               isDegraded,
	                               activeSharedTLog,
	                               enablePrimaryTxnSystemHealthCheck);

	// start tlog.
	state InitializeTLogRequest initTLogReq = InitializeTLogRequest();
	if (initReq != nullptr) {
		initTLogReq = *initReq;
	} else {
		std::vector<Tag> tags;
		for (uint32_t tagID = 0; tagID < pTLogTestContext->numTagsPerServer; tagID++) {
			Tag tag(pTLogTestContext->tagLocality, tagID);
			tags.push_back(tag);
		}
		initTLogReq.epoch = 1;
		initTLogReq.allTags = tags;
		initTLogReq.isPrimary = true;
		initTLogReq.locality = 0;
		initTLogReq.recoveryTransactionVersion = 1;
	}
	// initTLogReq.clusterId = deterministicRandom()->randomUniqueID();
	TLogInterface interface = wait(promiseStream.getReply(initTLogReq));
	pTLogContext->TestTLogInterface = interface;
	pTLogContext->init = promiseStream;

	// inform other actors tLog is ready.
	pTLogContext->TLogCreated.send(true);

	TraceEvent("TestTLogServerInitializedTLog", pTLogContext->tLogID);

	// wait for either test completion or abnormal failure.
	choose {
		when(wait(tl)) {}
		when(bool testCompleted = wait(pTLogContext->TestTLogServerCompleted.getFuture())) {
			ASSERT_EQ(testCompleted, true);
		}
	}

	wait(delay(1.0));

	// delete old disk queue files
	deleteFile(diskQueueFilename + "0." + tLogOptions.diskQueueExtension);
	deleteFile(diskQueueFilename + "1." + tLogOptions.diskQueueExtension);
	deleteFile(kvStoreFilename + "0." + tLogOptions.kvStoreExtension);
	deleteFile(kvStoreFilename + "1." + tLogOptions.kvStoreExtension);

	TraceEvent("ExitGetTLogCreateActor", pTLogContext->tLogID);

	return Void();
}

// sent commits through TLog interface.
ACTOR Future<Void> TLogTestContext::sendCommitMessages(TLogTestContext* pTLogTestContext, uint16_t processID) {
	state std::shared_ptr<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[processID];
	bool tLogReady = wait(pTLogContext->TLogStarted.getFuture());
	ASSERT_EQ(tLogReady, true);

	TraceEvent("TestTLogEnterSendCommits");

	state Version prev = 0;
	state Version next = 1;
	state int i = 0;
	for (; i < pTLogTestContext->numCommits; i++) {
		Standalone<StringRef> key = StringRef(format("key %d", i));
		Standalone<StringRef> val = StringRef(format("value %d", i));
		MutationRef m(MutationRef::Type::SetValue, key, val);

		// build commit request
		LogPushData toCommit(pTLogTestContext->ls, pTLogTestContext->numLogServers);
		// UID spanID = deterministicRandom()->randomUniqueID();
		toCommit.addTransactionInfo(SpanContext());
		std::vector<Tag> tags;

		// Currently every commit will use all tags, which is not representative of real-world scenarios.
		// TODO randomize tags to mimic real workloads
		for (uint32_t tagID = 0; tagID < pTLogTestContext->numTagsPerServer; tagID++) {
			Tag tag(pTLogTestContext->tagLocality, tagID);
			tags.push_back(tag);
		}
		toCommit.addTags(tags);
		toCommit.writeTypedMessage(m);

		int location = 0;
		Standalone<StringRef> msg = toCommit.getMessages(location);

		// send commit and wait for reply.
		::TLogCommitRequest request(SpanContext(),
		                            msg.arena(),
		                            prev,
		                            next,
		                            prev,
		                            prev,
		                            msg,
		                            pTLogTestContext->numLogServers,
		                            deterministicRandom()->randomUniqueID());
		::TLogCommitReply reply = wait(pTLogContext->TestTLogInterface.commit.getReply(request));
		ASSERT_LE(reply.version, next);
		prev++;
		next++;
	}

	TraceEvent("TestTLogExitSendCommits");

	return Void();
}

// Send pushes through the LogSystem interface. There is one of these actors for each log server.
ACTOR Future<Void> TLogTestContext::sendPushMessages(TLogTestContext* pTLogTestContext) {

	TraceEvent("TestTLogServerEnterPush", pTLogTestContext->workerID);

	state uint16_t logID = 0;
	for (logID = 0; logID < pTLogTestContext->numLogServers; logID++) {
		state std::shared_ptr<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[logID];
		bool tLogReady = wait(pTLogContext->TLogStarted.getFuture());
		ASSERT_EQ(tLogReady, true);
	}

	state Version prev = 0;
	state Version next = 1;
	state int i = 0;
	for (; i < pTLogTestContext->numCommits; i++) {
		Standalone<StringRef> key = StringRef(format("key %d", i));
		Standalone<StringRef> val = StringRef(format("value %d", i));
		MutationRef m(MutationRef::Type::SetValue, key, val);
		TraceEvent("TestTLogPush").detail("I", next);

		// build commit request
		LogPushData toCommit(pTLogTestContext->ls, pTLogTestContext->numLogServers /* tLogCount */);
		// UID spanID = deterministicRandom()->randomUniqueID();
		toCommit.addTransactionInfo(SpanContext());

		// for each tag
		for (uint32_t tagID = 0; tagID < pTLogTestContext->numTagsPerServer; tagID++) {
			Tag tag(pTLogTestContext->tagLocality, tagID);
			std::vector<Tag> tags = { tag };
			toCommit.addTags(tags);
			toCommit.writeTypedMessage(m);
		}
		Future<Version> loggingComplete = pTLogTestContext->ls->push(prev, next, prev, prev, toCommit, SpanContext());
		Version ver = wait(loggingComplete);
		ASSERT_LE(ver, next);
		prev++;
		next++;
	}

	TraceEvent("TestTLogServerExitPush", pTLogTestContext->workerID).detail("LogID", logID);

	return Void();
}

// send peek/pop through a given TLog interface (logGroupID) for a given tag (shardTag).
ACTOR Future<Void> TLogTestContext::peekCommitMessages(TLogTestContext* pTLogTestContext,
                                                       uint16_t logID,
                                                       uint32_t tagID) {
	state std::shared_ptr<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[logID];
	bool tLogReady = wait(pTLogContext->TLogStarted.getFuture());
	ASSERT_EQ(tLogReady, true);

	// peek from the same tag
	state Tag tag(pTLogTestContext->tagLocality, tagID);

	TraceEvent("TestTLogServerEnterPeek", pTLogTestContext->workerID).detail("LogID", logID).detail("Tag", tag);

	state Version begin = 1;
	state int i;
	for (i = 0; i < pTLogTestContext->numCommits; i++) {
		// wait for next message commit
		::TLogPeekRequest request(begin, tag, false, false);
		::TLogPeekReply reply = wait(pTLogContext->TestTLogInterface.peekMessages.getReply(request));
		TraceEvent("TestTLogServerTryValidateDataOnPeek", pTLogTestContext->workerID)
		    .detail("B", reply.begin.present() ? reply.begin.get() : -1);

		// validate versions
		ASSERT_GE(reply.maxKnownVersion, i);

		// deserialize package, first the version header
		ArenaReader rd = ArenaReader(reply.arena, reply.messages, AssumeVersion(g_network->protocolVersion()));
		ASSERT_EQ(*(int32_t*)rd.peekBytes(4), VERSION_HEADER);
		int32_t dummy; // skip past VERSION_HEADER
		Version ver;
		rd >> dummy >> ver;

		// deserialize transaction header
		int32_t messageLength;
		uint16_t tagCount;
		uint32_t sub = 1;
		if (FLOW_KNOBS->WRITE_TRACING_ENABLED) {
			rd >> messageLength >> sub >> tagCount;
			rd.readBytes(tagCount * sizeof(Tag));

			// deserialize span id
			if (sub == 1) {
				SpanContextMessage contextMessage;
				rd >> contextMessage;
			}
		}

		// deserialize mutation header
		if (sub == 1) {
			rd >> messageLength >> sub >> tagCount;
			rd.readBytes(tagCount * sizeof(Tag));
		}
		// deserialize mutation
		MutationRef m;
		rd >> m;

		// validate data
		Standalone<StringRef> expectedKey = StringRef(format("key %d", i));
		Standalone<StringRef> expectedVal = StringRef(format("value %d", i));
		ASSERT_WE_THINK(m.param1 == expectedKey);
		ASSERT_WE_THINK(m.param2 == expectedVal);

		TraceEvent("TestTLogServerValidatedDataOnPeek", pTLogTestContext->workerID)
		    .detail("Commit count", i)
		    .detail("LogID", logID)
		    .detail("TagID", tag);

		// go directly to pop as there is no SS.
		::TLogPopRequest requestPop(begin, begin, tag);
		wait(pTLogContext->TestTLogInterface.popMessages.getReply(requestPop));

		begin++;
	}

	TraceEvent("TestTLogServerExitPeek", pTLogTestContext->workerID).detail("LogID", logID).detail("TagID", tag);

	return Void();
}

// wait for all tLogs to be created. Then build a single tLog server, then
// signal transactions can start.
ACTOR Future<Void> getTLogGroupActor(std::shared_ptr<TLogTestContext> pTLogTestContext) {
	// create tLog
	state std::shared_ptr<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[0];
	bool isCreated = wait(pTLogContext->TLogCreated.getFuture());
	ASSERT_EQ(isCreated, true);
	TraceEvent("TestTLogCreatedTLog");

	// setup log system and tlog group
	pTLogTestContext->tLogSet.tLogs.push_back(OptionalInterface<TLogInterface>(pTLogContext->TestTLogInterface));
	pTLogTestContext->tLogSet.tLogLocalities.push_back(LocalityData());
	pTLogTestContext->tLogSet.tLogPolicy = Reference<IReplicationPolicy>(new PolicyOne());
	pTLogTestContext->tLogSet.locality = 0;
	pTLogTestContext->tLogSet.isLocal = true;
	pTLogTestContext->tLogSet.tLogVersion = TLogVersion::V6;

	pTLogTestContext->dbInfo.logSystemConfig.tLogs.push_back(pTLogTestContext->tLogSet);
	// pTLogTestContext->dbInfo.logSystemConfig.oldTLogs.push_back(pTLogTestContext->oldTLogSet);
	PromiseStream<Future<Void>> promises;
	pTLogTestContext->ls =
	    ILogSystem::fromServerDBInfo(pTLogTestContext->logID, pTLogTestContext->dbInfo, false, promises);

	// start transactions
	pTLogTestContext->pTLogContextList[0]->TLogStarted.send(true);
	Future<Void> commit = pTLogTestContext->sendCommitMessages();
	Future<Void> peek = pTLogTestContext->peekCommitMessages();
	wait(commit && peek);

	// tell tLog actor to initiate shutdown.
	pTLogTestContext->pTLogContextList[0]->TestTLogServerCompleted.send(true);

	TraceEvent("TestTLogExitTLogGroupActor");

	return Void();
}

// wait for all tLogs to be created. Then start actor to do push, then
// start actors to do peeks, then signal transactions can start.
ACTOR Future<Void> runTestsTLogGroupActors(std::shared_ptr<TLogTestContext> pTLogTestContext) {
	// create tLog
	state uint16_t processID = 0;
	// state std::vector<Future<Void>> logRouterActors;
	state TLogSet tLogSet;

	for (; processID < pTLogTestContext->numLogServers; processID++) {
		state std::shared_ptr<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[processID];
		bool isCreated = wait(pTLogContext->TLogCreated.getFuture());
		ASSERT_EQ(isCreated, true);

		// setup log system and tlog group
		tLogSet.tLogs.push_back(OptionalInterface<TLogInterface>(pTLogContext->TestTLogInterface));
		tLogSet.tLogLocalities.push_back(LocalityData());
		tLogSet.tLogPolicy = Reference<IReplicationPolicy>(new PolicyOne());
		tLogSet.locality = 0;
		tLogSet.isLocal = true;
		tLogSet.tLogVersion = TLogVersion::V6;
	}
	pTLogTestContext->dbInfo.logSystemConfig.tLogs.push_back(tLogSet);
	for (processID = 0; processID < pTLogTestContext->numLogServers; processID++) {
		std::shared_ptr<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[processID];
		// start transactions
		pTLogContext->TLogStarted.send(true);
	}

	PromiseStream<Future<Void>> promises;
	pTLogTestContext->ls =
	    ILogSystem::fromServerDBInfo(pTLogTestContext->logID, pTLogTestContext->dbInfo, false, promises);

	std::vector<Future<Void>> actors;

	// start push actor
	actors.emplace_back(pTLogTestContext->sendPushMessages());

	// start peek actors
	for (processID = 0; processID < pTLogTestContext->numLogServers; processID++) {
		for (uint32_t tagID = 0; tagID < pTLogTestContext->numTagsPerServer; tagID++) {
			actors.emplace_back(pTLogTestContext->peekCommitMessages(processID, tagID));
		}
	}

	wait(waitForAll(actors));

	// tell tLog actors to initiate shutdown.
	for (processID = 0; processID < pTLogTestContext->numLogServers; processID++) {
		pTLogTestContext->pTLogContextList[processID]->TestTLogServerCompleted.send(true);
	}

	return Void();
}

// wait for all tLogs to be created. Then start actor to do push, then
// start actors to do peeks, then signal transactions can start.

ACTOR Future<Void> buildTLogSet(std::shared_ptr<TLogTestContext> pTLogTestContext) {
	state TLogSet tLogSet;
	state uint16_t processID = 0;

	for (; processID < pTLogTestContext->numLogServers; processID++) {
		state std::shared_ptr<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[processID];
		bool isCreated = wait(pTLogContext->TLogCreated.getFuture());
		ASSERT_EQ(isCreated, true);

		// setup log system and tlog group
		tLogSet.tLogs.push_back(OptionalInterface<TLogInterface>(pTLogContext->TestTLogInterface));
		tLogSet.tLogLocalities.push_back(LocalityData());
		tLogSet.tLogPolicy = Reference<IReplicationPolicy>(new PolicyOne());
		tLogSet.locality = 0;
		tLogSet.isLocal = true;
		tLogSet.tLogVersion = TLogVersion::V6;
		tLogSet.tLogReplicationFactor = 1;
	}
	pTLogTestContext->dbInfo.logSystemConfig.tLogs.push_back(tLogSet);
	for (processID = 0; processID < pTLogTestContext->numLogServers; processID++) {
		std::shared_ptr<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[processID];
		// start transactions
		pTLogContext->TLogStarted.send(true);
	}
	return Void();
}

// create actors and return them in a list.
std::vector<Future<Void>> startTLogTestActors(const UnitTestParameters& params) {
	TraceEvent("TestTLogServerEnterStartTestActors");

	std::vector<Future<Void>> actors;
	std::shared_ptr<TLogTestContext> pTLogTestContext = initTLogTestContext(TestTLogOptions(params));
	const TestTLogOptions& tLogOptions = pTLogTestContext->tLogOptions;
	std::shared_ptr<TLogContext> pTLogContext(new TLogContext());
	pTLogTestContext->pTLogContextList.push_back(pTLogContext);

	// Create a single TLog
	actors.emplace_back(getTLogCreateActor(pTLogTestContext, tLogOptions, 0 /* processID */));

	// Create TLog group to drive tansactions
	actors.emplace_back(getTLogGroupActor(pTLogTestContext));
	TraceEvent("TestTLogServerExitStartTestActors");

	return actors;
}

// create actors and return them in a list.
std::vector<Future<Void>> startTLogGroupActors(const UnitTestParameters& params) {
	std::vector<Future<Void>> actors;
	std::shared_ptr<TLogTestContext> pTLogTestContext = initTLogTestContext(TestTLogOptions(params));
	const TestTLogOptions& tLogOptions = pTLogTestContext->tLogOptions;

	// Create one TLog for each log server. Only a single group of log servers is supported.
	for (int processID = 0; processID < pTLogTestContext->numLogServers; processID++) {
		std::shared_ptr<TLogContext> pTLogContext(new TLogContext(processID));
		pTLogTestContext->pTLogContextList.push_back(pTLogContext);
	}
	for (int processID = 0; processID < pTLogTestContext->numLogServers; processID++) {
		actors.emplace_back(getTLogCreateActor(pTLogTestContext, tLogOptions, processID));
	}

	// start fake proxy, which will create peek and commit actors.
	actors.emplace_back(runTestsTLogGroupActors(pTLogTestContext));
	return actors;
}

// This test creates tLogs and pushes data to them. The tLogs are then locked. A new "generation"
// of tLogs is then created. These enter recover mode and pull data from the old generation.
// The data is peeked from the new generation and validated.

ACTOR Future<Void> startTestsTLogRecoveryActors(UnitTestParameters params) {
	state std::vector<Future<Void>> tLogActors;
	state std::shared_ptr<TLogTestContext> pTLogTestContextEpochOne = initTLogTestContext(TestTLogOptions(params));
	const TestTLogOptions& tLogOptions = pTLogTestContextEpochOne->tLogOptions;
	state uint16_t instanceIndex = 0;

	TraceEvent("TestTLogServerEnterRecoveryTest");

	// Create the first "old" generation of tLogs
	std::shared_ptr<TLogContext> pTLogContext(new TLogContext(instanceIndex));
	pTLogTestContextEpochOne->pTLogContextList.push_back(pTLogContext);
	tLogActors.emplace_back(getTLogCreateActor(pTLogTestContextEpochOne, tLogOptions, instanceIndex));

	// wait for tLogs to be created, and signal pushes can start
	wait(buildTLogSet(pTLogTestContextEpochOne));

	PromiseStream<Future<Void>> promises;
	pTLogTestContextEpochOne->ls = ILogSystem::fromServerDBInfo(
	    pTLogTestContextEpochOne->logID, pTLogTestContextEpochOne->dbInfo, false, promises);

	// Push commits, but do not peek from them. They will be peeked by the next
	// generation of tLogs.
	wait(pTLogTestContextEpochOne->sendPushMessages());

	// Done with old generation. Lock the old generation of tLogs.
	TLogLockResult data = wait(
	    pTLogTestContextEpochOne->pTLogContextList[instanceIndex]->TestTLogInterface.lock.getReply<TLogLockResult>());
	TraceEvent("TestTLogLockResult").detail("KCV", data.knownCommittedVersion);

	// Setup configuration for a new generation of tLogs
	state std::shared_ptr<TLogTestContext> pTLogTestContextEpochTwo = initTLogTestContext(TestTLogOptions(params), 0);

	// next epoch
	pTLogTestContextEpochTwo->epoch = pTLogTestContextEpochOne->epoch + 1;

	std::shared_ptr<TLogContext> pNewTLogContext(new TLogContext(instanceIndex));
	pTLogTestContextEpochTwo->pTLogContextList.push_back(pNewTLogContext);

	// The InitializeTLogRequest includes what versions should be recovered from
	// the previous generation. Those values are manually set here.
	InitializeTLogRequest req;
	req.recruitmentID = pTLogTestContextEpochTwo->dbInfo.logSystemConfig.recruitmentID;
	req.recoverAt = 3;
	req.startVersion = 2;
	req.remoteTag = Tag(tagLocalityRemoteLog, 0);
	req.recoveryTransactionVersion = 1;
	req.knownCommittedVersion = 1;
	req.epoch = pTLogTestContextEpochTwo->epoch;
	req.logVersion = TLogVersion::V6;
	req.locality = 0;
	req.isPrimary = true;
	req.logRouterTags = 0; // the number of LR, not spun up for recovery
	req.recoverTags = { Tag(0, 0) };
	req.recoverFrom = pTLogTestContextEpochOne->dbInfo.logSystemConfig;
	req.recoverFrom.logRouterTags = 0; // move

	// Describe the old log generation. There is one such structure for each generation.
	OldTLogConf oldTLogConf;
	oldTLogConf.tLogs = pTLogTestContextEpochOne->dbInfo.logSystemConfig.tLogs;
	oldTLogConf.tLogs[0].locality = 0;
	oldTLogConf.tLogs[0].isLocal = true;
	oldTLogConf.epochBegin = 1;
	oldTLogConf.epochEnd = 3;
	oldTLogConf.logRouterTags = 0;
	oldTLogConf.recoverAt = 1; // recoverAt version for old epoch, not new one
	oldTLogConf.epoch = 1; // old epoch, not new one
	pTLogTestContextEpochTwo->dbInfo.logSystemConfig.oldTLogs.push_back(oldTLogConf);
	pTLogTestContextEpochTwo->tagLocality = 0;

	const TestTLogOptions& tLogOptions = pTLogTestContextEpochTwo->tLogOptions;

	// Create the new generation's tLogs
	tLogActors.emplace_back(getTLogCreateActor(pTLogTestContextEpochTwo,
	                                           tLogOptions,
	                                           instanceIndex,
	                                           &req,
	                                           pTLogTestContextEpochOne->pTLogContextList[instanceIndex]->tLogID));

	state std::shared_ptr<TLogContext> pTLogContext = pTLogTestContextEpochTwo->pTLogContextList[0];
	bool isCreated = wait(pTLogContext->TLogCreated.getFuture());
	ASSERT_EQ(isCreated, true);
	pTLogContext->TLogStarted.send(true);

	TraceEvent("TestTLogStartTestsTLogRecovery");

	// read data from old generation of tLogs
	wait(pTLogTestContextEpochTwo->peekCommitMessages(0, 0));

	// signal that the old tLogs can be destroyed
	pTLogTestContextEpochTwo->pTLogContextList[instanceIndex]->TestTLogServerCompleted.send(true);
	pTLogTestContextEpochOne->pTLogContextList[instanceIndex]->TestTLogServerCompleted.send(true);

	// wait for the tLogs to destruct
	wait(waitForAll(tLogActors));

	TraceEvent("TestTLogServerExitRecoveryTest");

	return Void();
}

// test a single tLog
TEST_CASE("/fdbserver/test/TestSingleTLog") {
	FlowTransport::createInstance(false, 1, WLTOKEN_RESERVED_COUNT);
	wait(waitForAll(startTLogTestActors(params)));
	return Void();
}

// test a group of tLogs
TEST_CASE("/fdbserver/test/TestManyTLogs") {
	FlowTransport::createInstance(false, 1, WLTOKEN_RESERVED_COUNT);
	wait(waitForAll(startTLogGroupActors(params)));
	return Void();
}

// test a group of tLogs and recovery
TEST_CASE("/fdbserver/test/TestTLogRecovery") {
	FlowTransport::createInstance(false, 1, WLTOKEN_RESERVED_COUNT);
	wait(startTestsTLogRecoveryActors(params));
	return Void();
}
