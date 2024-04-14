/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class CalculateAverage_jdoit {

    private final static String FILE = "./measurements.txt";

    private final static int MIN_FILE_CHUNK_SIZE = 256 * 1024 * 1024;

    public static void main(String[] args) throws IOException, InterruptedException {
        new CalculateAverage_jdoit().run();
    }

    private void run() throws IOException, InterruptedException {
        readMeasurements();
    }

    private void readMeasurements() throws IOException, InterruptedException {
        try (RandomAccessFile file = new RandomAccessFile(new File(FILE), "r")) {
            final long fileSize = file.length();

            FileChannel fileChannel = file.getChannel();

            int chunkAmountCandidate = max(Math.toIntExact(fileSize / MIN_FILE_CHUNK_SIZE), 1);
            int chunkAmount = min(Runtime.getRuntime().availableProcessors() * 10, chunkAmountCandidate);

            int chunkSize = Math.toIntExact(fileSize / chunkAmount);

            ExecutorService executor = Executors.newFixedThreadPool(chunkAmount);

            long endPosition = 0;
            List<Future<Map<String, MeasurmentStat>>> tasks = new ArrayList<>();
            for (var i = 0; i <= chunkAmount; i++) {

                long startPosition = min(endPosition, fileSize);

                file.seek(min(startPosition + chunkSize - 1, fileSize));

                int value = 0;
                do {
                    value = file.read();
                } while (value != '\n' && value != -1);

                endPosition = file.getFilePointer();

                ParsingTask task = new ParsingTask(fileChannel, startPosition, endPosition);

                tasks.add(executor.submit(task::run));
            }

            Map<String, MeasurmentStat> result = new TreeMap<>();
            tasks.forEach(task -> {
                try {
                    var measurments = task.get();
                    for (var measurment : measurments.entrySet()) {
                        result.merge(measurment.getKey(), measurment.getValue(), MeasurmentStat::merge);
                    }
                }
                catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            System.out.println(result);

            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.MINUTES);
        }
    }

    private int measurment(byte[] measurmentBuffer, int start, int length) {
        int result = 0;

        int sign = 1;
        if (measurmentBuffer[start] == '-') {
            sign = -1;
            start++;
        }
        while (start < length) {
            byte value = measurmentBuffer[start++];
            if (value != '.') {
                result = result * 10 + (value - '0');
            }
        }
        return result * sign;
    }

    private class ParsingTask {
        private final FileChannel fileChannel;
        private final long startPosition;
        private final long endPosition;

        public ParsingTask(FileChannel fileChannel, long startPosition, long endPosition) {
            this.fileChannel = fileChannel;
            this.startPosition = startPosition;
            this.endPosition = endPosition;
        }

        public Map<String, MeasurmentStat> run() {
            Map<String, MeasurmentStat> map = new HashMap<>(2000);
            try {
                MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, startPosition, endPosition - startPosition);

                int position = 0;
                while (position < buffer.limit()) {

                    byte value = 0;
                    int stationNamePos = 0;
                    int rowPosition = 0;

                    byte[] rowBuffer = new byte[200];

                    while ((value = buffer.get(position)) != ';') {
                        rowBuffer[rowPosition++] = value;
                        position++;
                    }

                    position++;

                    stationNamePos = rowPosition;
                    while ((value = buffer.get(position)) != '\n') {
                        rowBuffer[rowPosition++] = value;
                        position++;
                    }
                    position++;

                    String key = new String(rowBuffer, 0, stationNamePos, StandardCharsets.UTF_8);

                    MeasurmentStat stat = new MeasurmentStat(measurment(rowBuffer, stationNamePos, rowPosition));

                    map.merge(key, stat, MeasurmentStat::merge);
                }
            }
            catch (IOException e) {
                throw new IllegalStateException(e);
            }

            return map;
        }
    }

    private class MeasurmentStat {

        private int min;
        private int max;
        private int count;
        private int sum;

        public MeasurmentStat(int value) {
            this.count = 1;
            this.min = value;
            this.sum = value;
            this.max = value;
        }

        public MeasurmentStat merge(MeasurmentStat other) {
             max = max(this.max, other.max);
             min = min(this.min, other.min);
             count += other.count;
             sum += other.sum;

            return this;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(min / 10.0)
                    .append("/")
                    .append(Math.round(sum * 1.0 / count) / 10.0).append("/")
                    .append(max / 10.0);
            return sb.toString();
        }
    }
}
