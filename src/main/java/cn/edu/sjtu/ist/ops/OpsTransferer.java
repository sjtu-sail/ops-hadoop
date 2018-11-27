/*
 * Copyright 2018 SJTU IST Lab
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

package cn.edu.sjtu.ist.ops;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.JobConf;
import cn.edu.sjtu.ist.ops.common.TaskConf;
import cn.edu.sjtu.ist.ops.common.TaskPreAlloc;
import cn.edu.sjtu.ist.ops.common.ShuffleConf;;

class OpsTransferer extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(OpsTransferer.class);
    private final int num;
    private final OpsShuffleHandler shuffleHandler;

    public OpsTransferer(int num, OpsShuffleHandler shuffleHandler) {
        this.num = num;
        this.shuffleHandler = shuffleHandler;
    }

    @Override
    public void run() {
        this.setName("ops-transferer-[" + this.num + "]");
        try {
            logger.info("ops-transferer-[" + this.num + "] started");

            while (!Thread.currentThread().isInterrupted()) {
                ShuffleConf shuffle = null;
                shuffle = shuffleHandler.getPendingShuffle();
                doShuffle(shuffle);
            }
            // server.awaitTermination();
            // channel.wait();
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    private void doShuffle(ShuffleConf shuffle) {
        JobConf job = this.shuffleHandler.getJob(shuffle.getTask().getJobId());
        TaskPreAlloc preAlloc = job.getReducePreAlloc();

        logger.info("ops-transferer-[" + this.num + "] shuffle");

        // TODO: shuffle file based on index file
    }
}