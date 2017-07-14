/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.talefairy.flume;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * Utility class for users to generate their own keys. Any key can be used,
 * this is just a utility that provides a set of simple keys.
 */
public class SimpleRowKeyGenerator {

  private static final Logger logger = LoggerFactory.getLogger(SimpleRowKeyGenerator.class);

//  public static byte[] getUUIDKey(String prefix) throws UnsupportedEncodingException {
//    return (prefix + UUID.randomUUID().toString()).getBytes("UTF8");
//  }
//
//  public static byte[] getRandomKey(String prefix) throws UnsupportedEncodingException {
//    return (prefix + String.valueOf(new Random().nextLong())).getBytes("UTF8");
//  }
//
//  public static byte[] getTimestampKey(String prefix) throws UnsupportedEncodingException {
//    return (prefix + String.valueOf(System.currentTimeMillis())).getBytes("UTF8");
//  }
//
//  public static byte[] getNanoTimestampKey(String prefix) throws UnsupportedEncodingException {
//    return (prefix + String.valueOf(System.nanoTime())).getBytes("UTF8");
//  }

  /**
   * 自定义rowKey生成规则: 前缀+guid（用户唯一标示）
   * @param prefix
   * @return
   * @throws UnsupportedEncodingException
   */
  public static byte[] getKey(String prefix, byte[] event) throws UnsupportedEncodingException {
    String data = new String(event, "UTF-8");
    logger.info("data: " + data);
     JSONObject obj = JSON.parseObject(data);
    String guid = obj.getString("guid");
    if (guid.isEmpty()) {
      guid = "guidKey";
    }

    return (prefix + guid).getBytes("UTF8");
  }

}
