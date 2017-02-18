package com.library.hibernate;

import com.library.configs.HibernateConfig;
import com.library.datamodel.Constants.APIContentType;
import com.library.datamodel.Constants.NamedConstants;
import com.library.datamodel.Constants.TaskType;
import com.library.datamodel.dsm_bridge.TbTerminal;
import com.library.datamodel.model.v1_0.AdScreenOwner;
import com.library.datamodel.model.v1_0.AdText;
import com.library.datamodel.model.v1_0.BaseEntity;
import com.library.hibernate.utils.AuditTrailInterceptor;
import com.library.hibernate.utils.CallBack;
import com.library.sgsharedinterface.DBInterface;
import com.library.utilities.DbUtils;
import com.library.utilities.GeneralUtils;
import com.library.utilities.LoggerUtil;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.naming.NamingException;
import org.hibernate.CacheMode;
import org.hibernate.Criteria;
import org.hibernate.FetchMode;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.ImprovedNamingStrategy;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.IntegerType;
import org.hibernate.type.Type;

/**
 *
 * @author smallgod
 */
public final class CustomHibernate {

    private static final LoggerUtil LOGGER = new LoggerUtil(CustomHibernate.class);
    private static String hibernateFilePath;
    private final HibernateConfig hibernateConfig;
    private SessionFactory sessionFactory;

    public CustomHibernate(HibernateConfig hibernateConfig) {
        this.hibernateConfig = hibernateConfig;
        CustomHibernate.hibernateFilePath = hibernateConfig.getHibernateFilePath();

    }

    private SessionFactory getSessionFactory() {

        if (sessionFactory == null) {
            sessionFactory = ConfigureHibernate.getInstance().createSessionFactory();
        }

        return sessionFactory;
    }

    /**
     * Explicitly initiate the DB resources
     *
     * @return
     */
    public boolean initialiseDBResources() {

        boolean initialised = Boolean.TRUE;

        sessionFactory = ConfigureHibernate.getInstance().createSessionFactory();

        if (sessionFactory == null) {
            initialised = Boolean.FALSE;
        }

        return initialised;
    }

    /**
     * Close the hibernate session factory after use
     */
    public void releaseDBResources() {

        if (getSessionFactory() != null && !getSessionFactory().isClosed()) {
            getSessionFactory().close();

            LOGGER.debug("Closing Hibernate SessionFactory...");
        } else {
            LOGGER.debug(">>>>>> called closeHibernateSessionFactory() but SessionFactory already CLOSED!!!!");
        }
    }

    //Session methods
    private Session getSession() {

        Session session = null;

        try {

            session = getSessionFactory().getCurrentSession();

            if (!session.isOpen()) {
                session = getSessionFactory().openSession();
            }

        } catch (HibernateException he) {
            LOGGER.error("Hibernate exception: " + he.getMessage());

        }
        return session;
    }

    private StatelessSession getStatelessSession() {

        StatelessSession statelessSession;

        try {
            statelessSession = getSessionFactory().openStatelessSession();
            LOGGER.debug("openned stateless session");
        } catch (HibernateException he) {
            LOGGER.error("Hibernate exception openning stateless session: " + he.getMessage());
            throw new NullPointerException("Couldnot create open a statelesssession");
        }
        return statelessSession;
    }

    private static void closeSession(Session session) {

        if (session != null) {

            try {

                if (session.isConnected()) {
                    session.disconnect();
                }
                if (session.isOpen()) {
                    session.close();
                }

            } catch (HibernateException hbe) {
                LOGGER.error("Couldn't close Session: " + hbe.getMessage());
            }
        }
    }

    private static void closeSession(StatelessSession statelessSession) {

        if (statelessSession != null) {
            statelessSession.close();
        }
    }

    //CRUD methods
    /**
     * Method supports a callback function that can process multiple records
     * while saving in the database
     *
     * @param callBack
     * @return number of records processed and saved
     */
    public int processAndSave(CallBack callBack) {

        StatelessSession tempSession = getStatelessSession();
        Transaction transaction = null;
        int recordsProcessed = 0;

        try {

            transaction = tempSession.beginTransaction();

            recordsProcessed = callBack.processAndSaveMultipleRecords(tempSession);

            transaction.commit();

        } catch (HibernateException he) {
            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("hibernate exception inserting/updating: " + he.getMessage());
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("General exception saving object list: " + e.getCause().getMessage());
        } finally {
            closeSession(tempSession);
        }

        return recordsProcessed;
    }

    /**
     * Insert a list of entity records
     *
     * @param entityList to insert
     * @return if entity record has been inserted/saved
     */
    public boolean insertBulk(Set<DBInterface> entityList) {

        StatelessSession tempSession = getStatelessSession();
        Transaction transaction = null;
        boolean committed = false;

        try {

            transaction = tempSession.beginTransaction();
            for (DBInterface entity : entityList) {
                tempSession.insert(entity);
            }
            transaction.commit();
            committed = true;

        } catch (HibernateException he) {
            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("hibernate exception saving object list: " + he.getMessage());
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("General exception saving object list: " + e.getMessage());
        } finally {
            closeSession(tempSession);
        }

        return committed;
    }

    /**
     * Save a list of entity records while flushing a batch of records at a time
     * (to release memory)
     *
     * @param entityList to save
     * @return
     */
    public boolean saveBulk(Set<BaseEntity> entityList) {

        int insertCount = 0;

        Session tempSession = getSession();
        Transaction transaction = null;
        boolean committed = false;

        try {

            transaction = tempSession.beginTransaction();
            for (DBInterface entity : entityList) {

                tempSession.save(entity);

                if ((insertCount % NamedConstants.HIBERNATE_JDBC_BATCH) == 0) { // Same as the JDBC batch size
                    //flush a batch of inserts and release memory: Without the call to the flush method,
                    //your first-level cache would throw an OutOfMemoryException
                    tempSession.flush();
                    tempSession.clear();
                }

                insertCount++;
            }

            transaction.commit();
            committed = true;

        } catch (HibernateException he) {
            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("hibernate exception saving object list: " + he.getMessage());

            he.printStackTrace();

        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("General exception saving object list: " + e.getMessage());
            e.printStackTrace();

        } finally {

            closeSession(tempSession);
        }

        return committed;
    }

    /**
     * Save an entity record to a database
     *
     * @param entity to save
     * @return Database ID of saved object
     */
    public long saveEntity(DBInterface entity) {

        LOGGER.debug("============================= SAVE ENTITY BEGIN CALLED ================================");

        long entityId = 0L;
        Session tempSession = getSession();
        Transaction transaction = null;

        try {

            transaction = tempSession.beginTransaction();
            entityId = (long) tempSession.save(entity);

            transaction.commit();

        } catch (HibernateException he) {
            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("Error saving entity: " + he.getMessage());
            he.printStackTrace();
            //throw new MyCustomException("Hibernate Error saving object to DB", ErrorCode.PROCESSING_ERR, "Error saving: " + he.getMessage(), ErrorCategory.SERVER_ERR_TYPE);

        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("Error saving entity: " + e.getMessage());
            e.printStackTrace();
            //throw new MyCustomException("Error saving object to DB", ErrorCode.PROCESSING_ERR, "Error saving: " + e.getMessage(), ErrorCategory.SERVER_ERR_TYPE);

        } finally {
            closeSession(tempSession);
        }

        LOGGER.debug("================================= SAVE ENTITY END  ========================================");

        return entityId;
    }

    /**
     * Save an entity record to a database
     *
     * @param entity to save
     */
    public void saveOrUpdateEntity(DBInterface entity) {

        Session tempSession = getSession();
        Transaction transaction = null;

        try {

            transaction = tempSession.beginTransaction();
            tempSession.saveOrUpdate(entity);
            transaction.commit();

        } catch (HibernateException he) {
            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("Error saving entity: " + he.getMessage());
            he.printStackTrace();
            //throw new MyCustomException("Hibernate Error saving object to DB", ErrorCode.PROCESSING_ERR, "Error saving: " + he.getMessage(), ErrorCategory.SERVER_ERR_TYPE);

        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("Error saving entity: " + e.getMessage());
            e.printStackTrace();
            //throw new MyCustomException("Error saving object to DB", ErrorCode.PROCESSING_ERR, "Error saving: " + e.getMessage(), ErrorCategory.SERVER_ERR_TYPE);

        } finally {
            closeSession(tempSession);
        }
    }

    //check this method before using it, dont we need to use flush just like in bulkSave??
    public boolean bulkUpdate(Set<DBInterface> dbObjectList) {

        StatelessSession tempSession = getStatelessSession();
        Transaction transaction = null;
        boolean committed = false;

        try {

            transaction = tempSession.beginTransaction();

            //check this method before using it, dont we need to use flush just like in bulkSave??
            for (DBInterface dbObject : dbObjectList) {
                tempSession.update(dbObject);
            }
            transaction.commit();
            committed = true;

        } catch (HibernateException he) {
            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("hibernate exception UPDATING entity list: " + he.getMessage());
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("General exception UPDATING entity list: " + e.getMessage());
        } finally {
            closeSession(tempSession);
        }

        return committed;
    }

    /**
     * Update an entity in the database
     *
     * @param entity
     * @return
     */
    public boolean updateEntity(BaseEntity entity) {

        LOGGER.debug("Updating entity!");

        Session tempSession = getSession();
        Transaction transaction = null;
        boolean updated = false;

        try {
            transaction = tempSession.beginTransaction();
            tempSession.update(entity);
            //retrievedDatabaseModel = (T)getSession().get(persistentClass, objectId);
            //retrievedDatabaseModel = (T)session.merge(object);
            //tempSession.update(retrievedDatabaseModel);
            //retrievedDatabaseModel = dbObject;
            //tempSession.update(session.merge(retrievedDatabaseModel));
            //tempSession.update(retrievedDatabaseModel);
            tempSession.flush();
            transaction.commit();
            updated = Boolean.TRUE;

        } catch (HibernateException he) {

            updated = Boolean.FALSE;

            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("hibernate exception updating DB object: " + he.getMessage());
        } catch (Exception e) {

            updated = Boolean.FALSE;

            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("General exception updating DB object: " + e.getMessage());
        } finally {
            closeSession(tempSession);
        }

        return updated;
    }

    /**
     * Update a Terminal Entity
     *
     * @param assignTaskId
     * @param taskTypeEnum
     * @param oldTbTerminal
     */
    public void updateTerminalEntity(long assignTaskId, TaskType taskTypeEnum, TbTerminal oldTbTerminal) {

        LOGGER.debug("Updating terminal with assignLoopTaskId: " + assignTaskId);

        StatelessSession tempSession = getStatelessSession();

        Transaction transaction;
        boolean committed = false;

        //Type intType = IntegerType.INSTANCE;
        try {

            transaction = tempSession.beginTransaction();

            //String hqlQueryString = "SELECT rec.rowDetails FROM TemporaryRecords  as rec WHERE rec.fileID=:fileID and rec.generatedID NOT IN "+ "(SELECT rec.generatedID FROM TemporaryRecords where rec.generatedID Like '%F' or rec.generatedID like '%S' GROUP BY rec.generatedID HAVING COUNT(rec.generatedID) =:filecount)";
            //String sqlQueryString = "SELECT row_details FROM temporary_records WHERE file_id=:fileID AND generated_id NOT IN "+ "(SELECT generated_id FROM temporary_records WHERE generated_id Like '%F' OR generated_id LIKE '%S' GROUP BY generated_id HAVING COUNT(generated_id) =:filecount)";
            //String sqlQueryString = "UPDATE tb_terminal SET  NAME = :NAME, DESCP = :DESCP, GROUP_ID = :GROUP_ID, CITY_ID = :CITY_ID, ASSIGN_KERNEL = :ASSIGN_KERNEL, ASSIGN_APP = :ASSIGN_APP, ASSIGN_CONFIG_ID = :ASSIGN_CONFIG_ID, ASSIGN_LOOPTASK_ID = :ASSIGN_LOOPTASK_ID, ASSIGN_DEMANDTASK_ID = :ASSIGN_DEMANDTASK_ID, ASSIGN_PLUGINTASK_ID = :ASSIGN_PLUGINTASK_ID, REST_SCHEDULE = :REST_SCHEDULE, STANDBY_SCHEDULE = :STANDBY_SCHEDULE, CAPTURE_SCHEDULE = :CAPTURE_SCHEDULE, DEMAND_SCHEDULE = :DEMAND_SCHEDULE, SCHEDULE_VERSION = :SCHEDULE_VERSION, SUBTITLE = :SUBTITLE, SUBTITLE_VERSION = :SUBTITLE_VERSION WHERE CSTM_ID=:CSTM_ID AND DEV_ID = :DEV_ID";
            String taskIdToSet;
            switch (taskTypeEnum) {

                case LOOP:
                    taskIdToSet = "ASSIGN_LOOPTASK_ID";
                    break;

                case DEMAND:
                    taskIdToSet = "ASSIGN_DEMANDTASK_ID";
                    break;

                case PLUGIN:
                    taskIdToSet = "ASSIGN_PLUGINTASK_ID";
                    break;

                default:
                    taskIdToSet = "ASSIGN_LOOPTASK_ID";
                    break;
            }

            String sqlQueryString = "UPDATE tb_terminal SET " + taskIdToSet + " = :SET_TASK_ID WHERE CSTM_ID=:CSTM_ID AND DEV_ID = :DEV_ID";
            //Query query = session.createQuery(hqlQueryString);
            //Query query = session.createSQLQuery(sqlQueryString);
            SQLQuery query = tempSession.createSQLQuery(sqlQueryString);

            query.setParameter("SET_TASK_ID", DbUtils.ZeroToNull(assignTaskId));
            query.setParameter("CSTM_ID", oldTbTerminal.getId().getCstmId());
            query.setParameter("DEV_ID", oldTbTerminal.getId().getDevId());

            /*query.setParameter("ASSIGN_CONFIG_ID", DbUtils.ZeroToNull(oldTbTerminal.getTbConfig().getId().getConfigId()));
            query.setParameter("ASSIGN_DEMANDTASK_ID", DbUtils.ZeroToNull(oldTbTerminal.getTbDemandTask().getId().getTaskId()));
            query.setParameter("ASSIGN_PLUGINTASK_ID", DbUtils.ZeroToNull(oldTbTerminal.getTbPluginTask().getId().getTaskId()));

            
            query.setParameter("NAME", oldTbTerminal.getName());
            query.setParameter("DESCP", oldTbTerminal.getDescp());
            query.setParameter("CITY_ID", DbUtils.ZeroToNull(oldTbTerminal.getTbCity().getCityId()));
            query.setParameter("GROUP_ID", DbUtils.ZeroToNull(oldTbTerminal.getTbGroup().getId().getGroupId()));
            query.setParameter("ASSIGN_KERNEL", DbUtils.EmptyToNull(oldTbTerminal.getTbKernel().getVersion()));
            query.setParameter("ASSIGN_APP", DbUtils.EmptyToNull(oldTbTerminal.getTbApp().getVersion()));
            query.setParameter("REST_SCHEDULE", oldTbTerminal.getRestSchedule());
            query.setParameter("STANDBY_SCHEDULE", oldTbTerminal.getStandbySchedule());
            query.setParameter("CAPTURE_SCHEDULE", oldTbTerminal.getCaptureSchedule());
            query.setParameter("DEMAND_SCHEDULE", oldTbTerminal.getDemandSchedule());
            query.setParameter("SCHEDULE_VERSION", DbUtils.NullTo1970(oldTbTerminal.getScheduleVersion()));
            query.setParameter("SUBTITLE", oldTbTerminal.getSubtitle());
            query.setParameter("SUBTITLE_VERSION", DbUtils.NullTo1970(oldTbTerminal.getSubtitleVersion()));*/
            int updated = query.executeUpdate();
            transaction.commit();

            LOGGER.debug("Update query executed: " + updated);

            //SELECT generated_id, row_details FROM temporary_records WHERE generated_id IN  (SELECT generated_id FROM temporary_records GROUP BY generated_id HAVING COUNT(generated_id) =2)
            //Criteria.forClass(bob.class.getName())
            //Criteria criteria = session.createCriteria(classType);
            //criteria.add(Restrictions.or(Property.forName("col3").eq("value3"), Property.forName("col4").eq("value3")));       
            //we only want successful & Failed transactions (rest are exceptions)
            /*criteria.setProjection(Projections.property(columToFetch));
             criteria.add(
             //Restrictions.not(
                    
             Restrictions.or(
             Restrictions.ilike(restrictToPropertyName, "S", MatchMode.END),
             Restrictions.ilike(restrictToPropertyName, "F", MatchMode.END)
             )
             )
             .add(Restrictions.sqlRestriction("generated_id having count(generated_id) = ?", fileCount, intType));*/
        } catch (HibernateException he) {

            LOGGER.error("hibernate exception while updating tbTerminal: " + he.getMessage());
            he.printStackTrace();
        } catch (Exception e) {

            LOGGER.error("General exception while updating tbTerminal: " + e.getMessage());
            e.printStackTrace();
        } finally {
            closeSession(tempSession);
        }

    }

    public void bulkUpdateTerminalEntity(TaskType taskTypeEnum, Set<TbTerminal> oldTerminalEntityList) {

        StatelessSession tempSession = getStatelessSession();

        Transaction transaction;
        boolean committed = false;

        try {

            transaction = tempSession.beginTransaction();

            String taskIdToSet;
            switch (taskTypeEnum) {

                case LOOP:
                    taskIdToSet = "ASSIGN_LOOPTASK_ID";
                    break;

                case DEMAND:
                    taskIdToSet = "ASSIGN_DEMANDTASK_ID";
                    break;

                case PLUGIN:
                    taskIdToSet = "ASSIGN_PLUGINTASK_ID";
                    break;

                default:
                    taskIdToSet = "ASSIGN_LOOPTASK_ID";
                    break;
            }

            for (TbTerminal oldTbTerminal : oldTerminalEntityList) {

                String sqlQueryString = "UPDATE tb_terminal SET " + taskIdToSet + " = :SET_TASK_ID WHERE CSTM_ID=:CSTM_ID AND DEV_ID = :DEV_ID";
                SQLQuery query = tempSession.createSQLQuery(sqlQueryString);

                query.setParameter("SET_TASK_ID", DbUtils.ZeroToNull(NamedConstants.RESET_LOOP_TASKID));
                query.setParameter("CSTM_ID", oldTbTerminal.getId().getCstmId());
                query.setParameter("DEV_ID", oldTbTerminal.getId().getDevId());

                int updated = query.executeUpdate();
                LOGGER.debug("Update query executed: " + updated);
            }

            transaction.commit();
            LOGGER.debug("Update Tranasaction committed");

        } catch (HibernateException he) {

            LOGGER.error("hibernate exception while updating tbTerminal: " + he.getMessage());
            he.printStackTrace();
        } catch (Exception e) {

            LOGGER.error("General exception while updating tbTerminal: " + e.getMessage());
            e.printStackTrace();
        } finally {
            closeSession(tempSession);
        }

    }

    public <BaseEntity> Set<BaseEntity> fetchCorrespondingSet(Class entityType, String setPropertyName) {

        //StatelessSession session = getStatelessSession();
        Session session = getSession();
        Transaction transaction = null;

        Set<BaseEntity> results = new HashSet<>();

        try {

            transaction = session.beginTransaction();
            Criteria criteria = session.createCriteria(entityType);

            //criteria.add(Restrictions.gt("dealerId", dealerId));
            // this tells Hibernate that the makes must be fetched from the database
            // you must use the name of the annotated field in the Java class: dealerMakes
            criteria.setFetchMode(setPropertyName, FetchMode.JOIN);

            // Hibernate will return instances of Dealer, but it will return the same instance several times
            // once per make the dealer has. To avoid this, you must use a distinct root entity transformer
            criteria.setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);

            List<BaseEntity> records = criteria.list();
            results = GeneralUtils.convertListToSet(records);

            transaction.commit();

        } catch (HibernateException he) {

            he.printStackTrace();
            LOGGER.error("hibernate exception Fetching object list: " + he.getMessage());

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            e.printStackTrace();

            LOGGER.error("General exception Fetching object list: " + e.getMessage());

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        return results;

    }

    /**
     * Fetch records matching certain conditions
     *
     * @param <BaseEntity>
     * @param entityType
     * @param propertyNameValues
     * @return
     */
    public <BaseEntity> Set<BaseEntity> fetchBulk(Class entityType) {

        Session session = getSession();
        Transaction transaction = null;

        Set<BaseEntity> results = new HashSet<>();

        try {

            // Query query = session.createQuery("from Stock where stockCode = :code ");
            //query.setParameter("code", "7277");
            //List list = query.list();
            transaction = session.beginTransaction();
            Criteria criteria = session.createCriteria(entityType);
            criteria.setCacheMode(CacheMode.REFRESH);

            //criteria.addOrder(Order.asc(propertyName));
            // To-Do -> add the other parameters, e.g. orderby, etc
            ScrollableResults scrollableResults = criteria.scroll(ScrollMode.FORWARD_ONLY);

            int count = 0;
            while (scrollableResults.next()) {

                if (++count > 0 && count % 10 == 0) {

                    LOGGER.debug("Fetched " + count + " entities");
                    session.flush();
                    session.clear();
                }
                results.add((BaseEntity) scrollableResults.get()[0]);

            }

            //session.refresh(results);
            LOGGER.info("size of results: " + results.size());

            LOGGER.debug("DB Results from Fetch: " + Arrays.asList(results));

            transaction.commit();

        } catch (HibernateException he) {

            he.printStackTrace();
            LOGGER.error("hibernate exception Fetching object list: " + he.getMessage());

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            e.printStackTrace();

            LOGGER.error("General exception Fetching object list: " + e.getMessage());

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        return results;
    }

    /**
     * Fetch records matching certain conditions
     *
     * @param <BaseEntity>
     * @param entityType
     * @param propertyNameValues
     * @return
     */
    public <BaseEntity> Set<BaseEntity> fetchBulk(Class entityType, Map<String, Set<Object>> propertyNameValues) {

        Session session = getSession();
        Transaction transaction = null;

        Set<BaseEntity> results = new HashSet<>();

        try {

            transaction = session.beginTransaction();
            Criteria criteria = session.createCriteria(entityType);
            criteria.setCacheMode(CacheMode.REFRESH);

            propertyNameValues.entrySet().stream().forEach((entry) -> {

                String name = entry.getKey();
                Set<Object> values = entry.getValue();

                LOGGER.debug("Field Name  : " + name);
                LOGGER.debug("Field values: " + values);

                //if values set is empty or contains a '1' - we will select all records
                if (values == null || values.isEmpty() || values.contains(1)) {
                    LOGGER.info("No Restrictions on property: " + name + ", while Fetching: " + entityType.getName() + " objects.");
                } else {
                    criteria.add(Restrictions.in(name, values));
                }
            });

//            if(!isFetchAll){
//                criteria.add(Restrictions.allEq(propertyNameValues));
//            }
            //criteria.addOrder(Order.asc(propertyName));
            // To-Do -> add the other parameters, e.g. orderby, etc
            ScrollableResults scrollableResults = criteria.scroll(ScrollMode.FORWARD_ONLY);

            int count = 0;
            while (scrollableResults.next()) {
                if (++count > 0 && count % 10 == 0) {
                    LOGGER.debug("Fetched " + count + " entities");
                    session.flush();
                    session.clear();
                }
                results.add((BaseEntity) scrollableResults.get()[0]);

            }

            //session.refresh(results);
            LOGGER.info("size of results: " + results.size());

            LOGGER.debug("DB Results from Fetch: " + Arrays.asList(results));

//            List<BaseEntity> records = criteria.list();
//            results = GeneralUtils.convertListToSet(records);
            transaction.commit();

        } catch (HibernateException he) {

            he.printStackTrace();
            LOGGER.error("hibernate exception Fetching object list: " + he.getMessage());

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            e.printStackTrace();

            LOGGER.error("General exception Fetching object list: " + e.getMessage());

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        return results;
    }

    /**
     *
     * @param <BaseEntity>
     * @param entityType
     * @param propertyNameValues
     * @return
     */
    public <BaseEntity> Set<BaseEntity> fetchBulkGAVEHARDTIME(Class entityType, Map<String, Object[]> propertyNameValues) {

        //StatelessSession session = getStatelessSession();
        Session session = getSession();
        Transaction transaction = null;

        Set<BaseEntity> results = new HashSet<>();

        try {

            transaction = session.beginTransaction();
            Criteria criteria = session.createCriteria(entityType);

            LOGGER.debug("Property Values size: " + propertyNameValues.size());

            propertyNameValues.entrySet().stream().forEach((entry) -> {

                String name = entry.getKey();
                Object[] values = entry.getValue();

                LOGGER.debug("Field Name  : " + name);
                LOGGER.debug("Field values: " + Arrays.toString(values));

                //criteria.add(Restrictions.in(name, values)); //un-c0mment and sort out errors when r3ady 2do so
            });

//            if(!isFetchAll){
//                criteria.add(Restrictions.allEq(propertyNameValues));
//            }
            //criteria.addOrder(Order.asc(propertyName));
            // To-Do -> add the other parameters, e.g. orderby, etc
            ScrollableResults scrollableResults = criteria.scroll(ScrollMode.FORWARD_ONLY);

            int count = 0;
            while (scrollableResults.next()) {
                if (++count > 0 && count % 10 == 0) {
                    LOGGER.debug("Fetched " + count + " entities");
                    session.flush();
                    session.clear();
                }
                results.add((BaseEntity) scrollableResults.get()[0]);

            }

//            List<BaseEntity> records = criteria.list();
//            results = GeneralUtils.convertListToSet(records);
            transaction.commit();

        } catch (HibernateException he) {

            he.printStackTrace();
            LOGGER.error("hibernate exception Fetching object list: " + he.getMessage());

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            e.printStackTrace();

            LOGGER.error("General exception Fetching object list: " + e.getMessage());

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        return results;
    }

    public <BaseEntity> Set<BaseEntity> fetchBulk_TempSession(Class entityType, Map<String, Object[]> propertyNameValues) {

        StatelessSession session = getStatelessSession();

        Set<BaseEntity> results = new HashSet<>();

        try {

            Criteria criteria = session.createCriteria(entityType);

            propertyNameValues.entrySet().stream().forEach((entry) -> {

                String name = entry.getKey();
                Object[] values = entry.getValue();

                //criteria.add(Restrictions.in(name, values)); //un-c0mment and sort out errors when r3ady
            });

//            if(!isFetchAll){
//                criteria.add(Restrictions.allEq(propertyNameValues));
//            }
            //criteria.addOrder(Order.asc(propertyName));
            // To-Do -> add the other parameters, e.g. orderby, etc
            ScrollableResults scrollableResults = criteria.scroll(ScrollMode.FORWARD_ONLY);

            int count = 0;
            while (scrollableResults.next()) {
                if (++count > 0 && count % 10 == 0) {
                    LOGGER.debug("Fetched " + count + " entities");
                }
                results.add((BaseEntity) scrollableResults.get()[0]);

            }

        } catch (HibernateException he) {

            LOGGER.error("hibernate exception Fetching object list: " + he.getMessage());

        } catch (Exception e) {

            LOGGER.error("General exception Fetching object list: " + e.getMessage());

        } finally {
            closeSession(session);
        }

        return results;
    }

    /**
     * fetch bulk records that have a given property value
     *
     * @param entityType
     * @param propertyName
     * @param propertyValue
     * @return bulk of records fetched
     */
    public Set<DBInterface> fetchBulk(Class<DBInterface> entityType, String propertyName, Object propertyValue) {

        StatelessSession tempSession = getStatelessSession();
        Set<DBInterface> fetchedEntities = new HashSet<>();

        try {

            Criteria criteria = tempSession.createCriteria(entityType);
            criteria.add(Restrictions.eq(propertyName, propertyValue));

            ScrollableResults scrollableResults = criteria.scroll(ScrollMode.FORWARD_ONLY);

            int count = 0;
            while (scrollableResults.next()) {

                if ((count > 0) && (count % 10 == 0)) {
                    LOGGER.debug("Fetched " + count + " entities");
                }
                count++;
                fetchedEntities.add((DBInterface) scrollableResults.get()[0]);

            }
        } catch (HibernateException he) {

            LOGGER.error("hibernate exception saving object list: " + he.getMessage());
        } catch (Exception e) {

            LOGGER.error("General exception saving object list: " + e.getMessage());
        } finally {
            closeSession(tempSession);
        }

        return fetchedEntities;
    }

    /**
     *
     * @param entityType
     * @return
     */
    public <T> Set<T> fetchBulkStateless(Class<T> entityType) {

        StatelessSession tempSession = getStatelessSession();
        Set<T> fetchedEntities = new HashSet<>();

        try {

            Criteria criteria = tempSession.createCriteria(entityType);

            ScrollableResults scrollableResults = criteria.scroll(ScrollMode.FORWARD_ONLY);

            int count = 0;
            while (scrollableResults.next()) {

                if ((count > 0) && (count % 10 == 0)) {
                    LOGGER.debug("Fetched " + count + " entities");
                }
                count++;
                fetchedEntities.add((T) scrollableResults.get()[0]);

            }
        } catch (HibernateException he) {

            LOGGER.error("hibernate exception saving object list: " + he.getMessage());
        } catch (Exception e) {

            LOGGER.error("General exception saving object list: " + e.getMessage());
        } finally {
            closeSession(tempSession);
        }

        return fetchedEntities;
    }

    /**
     * Fetch only a single entity/object from the database
     *
     *
     * @param entityType
     * @param propertyName
     * @param propertyValue
     * @return
     */
    public DBInterface fetchEntity(Class entityType, String propertyName, Object propertyValue) {

        Session session = getSession();
        Transaction transaction = null;

        DBInterface result = null;

        try {

            transaction = session.beginTransaction();
            Criteria criteria = session.createCriteria(entityType);
            criteria.add(Restrictions.eq(propertyName, propertyValue));
            criteria.setMaxResults(1);

            result = (DBInterface) criteria.uniqueResult();

            transaction.commit();

        } catch (HibernateException he) {

            LOGGER.error("hibernate exception saving object list: " + he.getMessage());

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            LOGGER.error("General exception saving object list: " + e.getMessage());

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        return result;
    }

    /**
     * Fetch only a single entity/object from the database with a temp session
     *
     * @param entityType
     * @param propertyName
     * @param propertyValue
     * @return
     */
    public DBInterface fetchEntityTempSession(Class entityType, String propertyName, Object propertyValue) {

        StatelessSession tempSession = getStatelessSession();

        DBInterface result = null;

        try {

            Criteria criteria = tempSession.createCriteria(entityType);
            criteria.add(Restrictions.eq(propertyName, propertyValue));
            criteria.setMaxResults(1);

            result = (DBInterface) criteria.uniqueResult();

        } catch (HibernateException he) {

            LOGGER.error("hibernate exception saving object list: " + he.getMessage());
        } catch (Exception e) {

            LOGGER.error("General exception saving object list: " + e.getMessage());
        } finally {
            closeSession(tempSession);
        }

        return result;
    }

    /**
     * Fetch entire column without restrictions
     *
     * @param <T>
     * @param classType
     * @param columToFetch
     * @return
     */
    public <T> List<T> fetchOnlyColumn(Class classType, String columToFetch) {

        StatelessSession tempSession = getStatelessSession();
        List<T> results = new ArrayList<>();

        try {

            //Criteria.forClass(bob.class.getName())
            Criteria criteria = tempSession.createCriteria(classType);
            criteria.setProjection(Projections.property(columToFetch));
            //criteria.add(Restrictions.gt("id", 10));
            //criteria.add(Restrictions.eq(restrictToPropertyName, restrictionValue)); //transactions should belong to the same group
            //criteria.addOrder(Order.asc(propertyName));

            ScrollableResults scrollableResults = criteria.scroll(ScrollMode.FORWARD_ONLY);

            int count = 0;
            while (scrollableResults.next()) {
                if (++count > 0 && count % 10 == 0) {
                    LOGGER.debug("Fetched " + count + " entities");
                }
                results.add((T) scrollableResults.get()[0]);

            }

        } catch (HibernateException he) {

            LOGGER.error("hibernate exception while fetching: " + he.getMessage());
            he.printStackTrace();

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("General exception while fetching: " + e.getMessage());
        } finally {
            closeSession(tempSession);
        }

        return results;
    }

    private static final class ConfigureHibernate {

        private SessionFactory sessionFactory;

        private ConfigureHibernate() {

            try {
                configure();
            } catch (NamingException ex) {
                LOGGER.error("Naming exception during hibernate configuration: " + ex.getMessage());
            } catch (HibernateException ex) {
                LOGGER.error("Hibernate exception during hibernate configuration: " + ex.getMessage());
            }
        }

        private static class ConfigureHibernateSingletonHolder {

            private static final ConfigureHibernate INSTANCE = new ConfigureHibernate();
        }

        private static ConfigureHibernate getInstance() {
            return ConfigureHibernateSingletonHolder.INSTANCE;
        }

        private Object readResolve() {
            return getInstance();
        }

        private SessionFactory createSessionFactory() {

            if (sessionFactory == null || sessionFactory.isClosed()) {

                LOGGER.debug("SessionFactory is NULL or closed going to reconfigure");

                try {
                    configure();
                } catch (NamingException ex) {
                    LOGGER.error("Naming exception during hibernate configuration: " + ex.getMessage());
                } catch (HibernateException ex) {
                    LOGGER.error("Hibernate exception during hibernate configuration: " + ex.getMessage());
                    ex.printStackTrace();
                }

            } else {
                LOGGER.debug(">>>>>> We are GOOD, SessionFactory is not NULL and is OPEN");
            }

            return sessionFactory;
        }

        private void setSessionFactory(SessionFactory sessionFactory) {
            this.sessionFactory = sessionFactory;
        }

        private void configure() throws NamingException, HibernateException {

            LOGGER.debug(">>>>>>>> configure() method called here... IT IS HAPPENING, TAKE NOTE!!!!!!!");

            File file = new File(CustomHibernate.hibernateFilePath);

            Configuration configuration = new Configuration();
            configuration.configure(file);
            //Name tables with lowercase_underscore_separated
            //configuration.setNamingStrategy(ImprovedNamingStrategy.INSTANCE);
            //configuration.addResource(customTypesPropsFileLoc);
            configuration.setInterceptor(new AuditTrailInterceptor());
            //configuration.setInterceptor(new InterceptorClass());

            StandardServiceRegistryBuilder serviceRegistryBuilder = new StandardServiceRegistryBuilder().applySettings(configuration.getProperties());
            ServiceRegistry serviceRegistry = serviceRegistryBuilder.build();

            SessionFactory sessFactory = configuration.buildSessionFactory(serviceRegistry);

            setSessionFactory(sessFactory);
        }
    }
}
