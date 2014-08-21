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

import java.util.*;

import org.apache.usergrid.persistence.*;
import org.apache.usergrid.persistence.entities.Notification;
import org.apache.usergrid.persistence.entities.Notifier;
import org.apache.usergrid.persistence.entities.Receipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.usergrid.mq.Message;
import org.apache.usergrid.mq.QueueManager;
import org.apache.usergrid.mq.QueueResults;
import org.apache.usergrid.persistence.entities.Device;

/**
 * When all Tasks are "complete", this calls notifyAll(). Note: This may not
 * mean that all work is done, however, as delivery errors may come in after a
 * notification is "sent."
 */
public class TaskManager {

    private static final Logger LOG = LoggerFactory
            .getLogger(TaskManager.class);
    private final String path;

    private Notification notification;
    private AtomicLong successes = new AtomicLong();
    private AtomicLong failures = new AtomicLong();
    private AtomicLong skips = new AtomicLong();
    private QueueManager qm;
    private EntityManager em;

    public TaskManager(  EntityManager em, QueueManager qm, Notification notification,String queuePath) {
        this.em = em;
        this.qm = qm;
        this.path =queuePath;
        this.notification = notification;
    }

    public void skip(UUID deviceUUID) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("notification {} skipped device {}",
                    notification.getUuid(), deviceUUID);
        }
        skips.incrementAndGet();
        completed(null, null, deviceUUID, null);
    }

    public void completed(Notifier notifier, Receipt receipt, UUID deviceUUID,
                          String newProviderId) throws Exception {

        LOG.debug("REMOVED {}", deviceUUID);
        try {
            EntityRef deviceRef = new SimpleEntityRef(Device.ENTITY_TYPE,
                    deviceUUID);

            if (receipt != null) {
                LOG.debug("notification {} sent to device {}. saving receipt.",
                        notification.getUuid(), deviceUUID);
                successes.incrementAndGet();
                receipt.setSent(System.currentTimeMillis());
                this.saveReceipt(notification, deviceRef, receipt);
                LOG.debug("notification {} receipt saved for device {}",
                        notification.getUuid(), deviceUUID);
            }

            LOG.debug("notification {} removing device {} from remaining", notification.getUuid(), deviceUUID);
            qm.commitTransaction(path, receipt.getMessage().getTransaction(), null);

            if (newProviderId != null) {
                LOG.debug("notification {} replacing device {} notifierId", notification.getUuid(), deviceUUID);
                replaceProviderId(deviceRef, notifier, newProviderId);
            }

            LOG.debug("notification {} completed device {}", notification.getUuid(), deviceUUID);

        } finally {
            LOG.debug("COUNT is: {}", successes.get());
            // note: stats are transient for the duration of the batch
//                if (remaining.size() == 0) {
//                    long successesCopy = successes.get();
//                    long failuresCopy = failures.get();
//                    if (successesCopy > 0 || failuresCopy > 0 || skips.get()>0) {
//                        ns.finishedBatch(notification, successesCopy, failuresCopy);
//                    }
//                }

        }
    }

    public void failed(Notifier notifier, Receipt receipt, UUID deviceUUID, Object code,
                       String message) throws Exception {

        try {
            if (LOG.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("notification ").append(notification.getUuid());
                sb.append(" for device ").append(deviceUUID);
                sb.append(" got error ").append(code);
                LOG.debug(sb.toString());
            }

            failures.incrementAndGet();
            if (receipt.getUuid() != null) {
                successes.decrementAndGet();
            }
            receipt.setErrorCode(code);
            receipt.setErrorMessage(message);
            this.saveReceipt(notification, new SimpleEntityRef( Device.ENTITY_TYPE, deviceUUID), receipt);
            LOG.debug("notification {} receipt saved for device {}",  notification.getUuid(), deviceUUID);
        } finally {
            completed(notifier, null, deviceUUID, null);
        }
    }

    /*
    * called from TaskManager - creates a persistent receipt and updates the
    * passed one w/ the UUID
    */
    public void saveReceipt(EntityRef notification, EntityRef device,
                            Receipt receipt) throws Exception {
        if (receipt.getUuid() == null) {
            Receipt savedReceipt = em.create(receipt);
            receipt.setUuid(savedReceipt.getUuid());

            List<EntityRef> entities = Arrays.asList(notification, device);
            em.addToCollections(entities, Notification.RECEIPTS_COLLECTION,  savedReceipt);
        } else {
            em.update(receipt);
        }
    }

    protected void replaceProviderId(EntityRef device, Notifier notifier,
                                     String newProviderId) throws Exception {
        Object value = em.getProperty(device, notifier.getName()
                + NotificationsService.NOTIFIER_ID_POSTFIX);
        if (value != null) {
            em.setProperty(device, notifier.getName() + NotificationsService.NOTIFIER_ID_POSTFIX,  newProviderId);
        } else {
            value = em.getProperty(device, notifier.getUuid()
                    + NotificationsService.NOTIFIER_ID_POSTFIX);
            if (value != null) {
                em.setProperty(device,  notifier.getUuid() + NotificationsService.NOTIFIER_ID_POSTFIX, newProviderId);
            }
        }
    }


}