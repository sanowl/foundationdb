/*
 * TestTLogServer.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_TEST_TLOG_ACTOR_G_H)
#define FDBSERVER_TEST_TLOG_ACTOR_G_H
#include "fdbserver/TestTLogServer.actor.g.h"
#elif !defined(FDBSERVER_TEST_TLOG_ACTOR_H)
#define FDBSERVER_TEST_TLOG_ACTOR_H

#include <memory>
#include <unordered_map>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/ResolverInterface.h"
#include "fdbserver/TLogInterface.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h" // has to be last include

#pragma once

struct TestTLogOptions {
	std::string diskQueueBasename;
	std::string diskQueueExtension;
	std::string kvStoreFilename;
	std::string dataFolder;
	std::string kvStoreExtension;
	int64_t kvMemoryLimit;
	int numTagsPerServer;
	int numLogServers;
	int numCommits;

	explicit TestTLogOptions(const UnitTestParameters& params) {
		diskQueueBasename = params.get("diskQueueBasename").orDefault("folder");
		diskQueueExtension = params.get("diskQueueFileExtension").orDefault("ext");
		kvStoreExtension = params.get("diskQueueFileExtension").orDefault("fdr");
		kvStoreFilename = params.get("kvStoreFilename").orDefault("kvstore");
		dataFolder = params.get("dataFolder").orDefault("simfdb");
		kvMemoryLimit = params.getDouble("kvMemoryLimit").orDefault(0x500e6);
		numTagsPerServer = params.getInt("numTagsPerServer").orDefault(1);
		numLogServers = params.getInt("numLogServers").orDefault(1);
		numCommits = params.getInt("numCommits").orDefault(3);
	}
};

// state maintained for a single tlog.
struct TLogContext {
	UID tLogID;
	::TLogInterface TestTLogInterface;
	::TLogInterface MockLogRouterInterface;
	PromiseStream<InitializeTLogRequest> init;
	uint16_t tagProcessID;
	IKeyValueStore* persistentData;
	IDiskQueue* persistentQueue;

	// test states
	Promise<bool> TLogCreated;
	Promise<bool> TLogStarted;
	Promise<bool> TestTLogServerCompleted;

	TLogContext(int inProcessID = 0) : tagProcessID(inProcessID){};
};

// state maintained for all tlogs.
struct TLogTestContext {

	ACTOR static Future<Void> sendPushMessages(TLogTestContext* pTLogTestContext);

	Future<Void> sendPushMessages() { return sendPushMessages(this); }

	ACTOR static Future<Void> sendCommitMessages(TLogTestContext* pTLogTestContext, uint16_t processID);

	Future<Void> sendCommitMessages(uint16_t processID = 0) { return sendCommitMessages(this, processID); }

	Future<Void> peekCommitMessages(uint16_t logGroupID = 0, uint32_t tag = 0) {
		return peekCommitMessages(this, logGroupID, tag);
	}

	ACTOR static Future<Void> peekCommitMessages(TLogTestContext* pTLogTestContext, uint16_t logGroupID, uint32_t tag);

	TLogTestContext(TestTLogOptions& tLogOptions) : tLogOptions(tLogOptions), epoch(1) {}

	UID logID;
	UID workerID;

	// paramaters
	std::string diskQueueBasename;
	int numCommits;
	int numTagsPerServer;
	int numLogServers;

	// test driver state
	std::vector<std::shared_ptr<TLogContext>> pTLogContextList;
	TestTLogOptions tLogOptions;

	// fdb state
	Reference<ILogSystem> ls;
	ServerDBInfo dbInfo;
	Reference<AsyncVar<ServerDBInfo>> dbInfoRef;
	TLogSet tLogSet;
	Standalone<StringRef> dcID;
	Optional<Standalone<StringRef>> zoneID;
	int8_t tagLocality;
	int epoch;
};

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_TEST_TLOG_ACTOR_G_H
