/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package neptune.benchmark.generator;

import neptune.benchmark.common.DataType;
import neptune.benchmark.common.TableSchema;

import java.util.Arrays;
import java.util.List;

public class TransactionDataGenerator extends BaseDataGenerator {
  @Override
  protected void initTableSchema() {
    List<String> columns = Arrays.asList("id", "count");
    List<DataType> types = Arrays.asList(DataType.INT, DataType.INT);
    List<Boolean> notNull = Arrays.asList(true, true);
    schemaMap.put("tx", new TableSchema("tx", columns, types, notNull, 0));
  }

  @Override
  public Object generateValue(String tableName, int rowId, int columnId) {
    return rowId;
  }
}
