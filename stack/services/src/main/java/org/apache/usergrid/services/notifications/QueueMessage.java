/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.usergrid.services.notifications;

import org.apache.usergrid.mq.Message;
import org.apache.usergrid.persistence.EntityRef;

import java.util.UUID;

public class QueueMessage extends Message {

    static final String MESSAGE_PROPERTY_DEVICE_UUID = "deviceUUID";
    static final String MESSAGE_PROPERTY_APPLICATION_UUID = "applicationUUID";


    public QueueMessage() {
    }

    public QueueMessage(UUID applicationId,UUID notificationId,UUID deviceId){
        setApplicationId(applicationId);
        setDeviceId(deviceId);
    }



    public static QueueMessage generate(Message message){
        return new QueueMessage((UUID) message.getObjectProperty(MESSAGE_PROPERTY_APPLICATION_UUID),(UUID) message.getObjectProperty("notificationId"),(UUID) message.getObjectProperty(MESSAGE_PROPERTY_DEVICE_UUID));
    }

    public UUID getApplicationId() {
        return (UUID) this.getObjectProperty(MESSAGE_PROPERTY_APPLICATION_UUID);
    }
    public void setApplicationId(UUID applicationId){
        this.setProperty(MESSAGE_PROPERTY_APPLICATION_UUID,applicationId);
    }

    public UUID getDeviceId() {
        return (UUID) this.getObjectProperty(MESSAGE_PROPERTY_DEVICE_UUID);
    }
    public void setDeviceId(UUID deviceId){
        this.setProperty(MESSAGE_PROPERTY_DEVICE_UUID,deviceId);
    }

    public UUID getNotificationId(){
        return (UUID) this.getObjectProperty("notificationId");
    }

    public void setNotificationId(UUID notificationId){
        this.setProperty("notificationdId",notificationId);
    }
}
