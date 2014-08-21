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

import org.apache.usergrid.metrics.MetricsFactory;
import org.apache.usergrid.mq.Message;
import org.apache.usergrid.mq.QueueManager;
import org.apache.usergrid.mq.QueueQuery;
import org.apache.usergrid.mq.QueueResults;
import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.persistence.EntityManagerFactory;
import org.apache.usergrid.services.ServiceManager;
import org.apache.usergrid.services.ServiceManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import rx.*;
import rx.Observable;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Component( "notificationsQueueListener" )
public class QueueListener  {

    public static final String queuePath = "notifications/queuelistener";
    private static final Logger LOG = LoggerFactory.getLogger(QueueListener.class);

    @Autowired
    private MetricsFactory metricsService;

    @Autowired
    private ServiceManagerFactory smf;

    @Autowired
    private EntityManagerFactory emf;
    private QueueManager queueManager;
    private ServiceManager svcMgr;
    private Properties properties;

    public QueueListener() {
    }

    @PostConstruct
    void init() {
        svcMgr =  smf.getServiceManager(smf.getManagementAppId());
        queueManager = svcMgr.getQueueManager();
        properties = new Properties();
        try {
            properties.load(Thread.currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream("usergrid.properties"));
        } catch (Exception e) {
            LOG.error("Could not load props","");
        }

        run();
    }

    public void run(){

        AtomicInteger consecutiveExceptions = new AtomicInteger();
        // run until there are no more active jobs
        while ( true ) {
            try {
                QueueResults results = getDeliveryBatch(1000);
                List<Message> messages = results.getMessages();
                HashMap<UUID,List<QueueMessage>> queueMap = new HashMap<>();
                for(Message message : messages){
                    QueueMessage queueMessage = QueueMessage.generate(message);
                    if(queueMap.containsKey(queueMessage.getNotificationId())){
                        List<QueueMessage> queueMessages = queueMap.get(queueMessage);
                        queueMessages.add(queueMessage);
                    }else{
                        List<QueueMessage> queueMessages = new ArrayList<>();
                        queueMessages.add(queueMessage);
                        queueMap.put(queueMessage.getApplicationId(),queueMessages);
                    }
                }

                List<Observable> observables = new ArrayList<>();
                for(UUID applicationId : queueMap.keySet()){
                    EntityManager entityManager = emf.getEntityManager(applicationId);
                    ServiceManager serviceManager = smf.getServiceManager(applicationId);

                    NotificationsQueueManager manager = new NotificationsQueueManager(
                            new JobScheduler(serviceManager,entityManager),
                            entityManager,
                            properties,
                            queueManager,
                            metricsService
                    );

                   observables.add(manager.sendBatchToProviders(queueMap.get(applicationId), results.getPath()));
                }
                rx.Observable first = null;
                for(rx.Observable o : observables){
                    if (first == null) {
                        first = o;
                    } else {
                        first = Observable.merge(first, o);
                    }
                }
                first.toBlocking().lastOrDefault(null);
                consecutiveExceptions.set(0);
            }catch (Exception ex){
                LOG.error("failed to dequeue",ex);
                if(consecutiveExceptions.getAndIncrement() > 10){
                    LOG.error("killing message listener; too many failures");
                    break;
                }
            }
        }
    }

    public void queueMessage(QueueMessage message){
        queueManager.postToQueue(queuePath, message);

    }

    private QueueResults getDeliveryBatch(int batchSize) throws Exception {

        QueueQuery qq = new QueueQuery();
        qq.setLimit(batchSize);
        qq.setTimeout(TaskManager.MESSAGE_TRANSACTION_TIMEOUT);
        QueueResults results = queueManager.getFromQueue(queuePath, qq);
        LOG.debug("got batch of {} devices", results.size());
        return results;
    }

}
