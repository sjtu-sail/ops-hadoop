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

package cn.edu.sjtu.ist.ops.common;

import com.google.gson.Gson;

public class IndexRecord {
    private final long startOffset;
    private final long rawLength;
    private final long partLength;

    public IndexRecord(long startOffset, long rawLength, long partLength) {
        this.startOffset = startOffset;
        this.rawLength = rawLength;
        this.partLength = partLength;
    }

    public long getStartOffset() {
        return this.startOffset;
    }

    public long getRawLength() {
        return this.rawLength;
    }

    public long getPartLength() {
        return this.partLength;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
