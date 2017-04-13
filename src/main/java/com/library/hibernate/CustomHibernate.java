package com.library.hibernate;

import com.library.configs.HibernateConfig;
import com.library.customexception.MyCustomException;
import com.library.datamodel.Constants.ErrorCode;
import com.library.datamodel.Constants.FetchStatus;
import com.library.datamodel.Constants.NamedConstants;
import com.library.datamodel.Constants.TaskType;
import com.library.datamodel.dsm_bridge.TbTerminal;
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
import java.util.Collection;
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
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Property;
import org.hibernate.criterion.Restrictions;
import org.hibernate.service.ServiceRegistry;
import org.joda.time.LocalDate;

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
            LOGGER.error("Hibernate exception: " + he.toString());

        }
        return session;
    }

    private StatelessSession getStatelessSession() throws NullPointerException {

        StatelessSession statelessSession;

        try {
            statelessSession = getSessionFactory().openStatelessSession();
            LOGGER.debug("openned stateless session");
        } catch (HibernateException he) {
            LOGGER.error("Hibernate exception openning stateless session: " + he.toString());
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
                LOGGER.error("Couldn't close Session: " + hbe.toString());
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
    public int processAndSave(CallBack callBack) throws MyCustomException {

        StatelessSession tempSession = getStatelessSession();
        Transaction transaction = null;
        String errorDetails;

        try {

            transaction = tempSession.beginTransaction();

            int recordsProcessed = callBack.processAndSaveMultipleRecords(tempSession);

            transaction.commit();

            return recordsProcessed;

        } catch (HibernateException he) {

            errorDetails = "hibernate exception inserting/updating records in the database: " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }
        } catch (Exception e) {

            errorDetails = "General exception performing the processAndSave callback function that saves/updates records in the database: " + e.getCause().toString();

            if (transaction != null) {
                transaction.rollback();
            }
        } finally {
            closeSession(tempSession);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     * Insert a list of entity records
     *
     * @param entityList to insert
     * @return if entity record has been inserted/saved
     * @throws com.library.customexception.MyCustomException
     */
    public boolean insertBulk(Set<DBInterface> entityList) throws MyCustomException {

        StatelessSession tempSession = getStatelessSession();
        Transaction transaction = null;
        String errorDetails;

        try {

            transaction = tempSession.beginTransaction();
            for (DBInterface entity : entityList) {
                tempSession.insert(entity);
            }
            transaction.commit();
            return Boolean.TRUE;

        } catch (HibernateException he) {

            errorDetails = "hibernate exception saving records in the database: " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }
        } catch (Exception e) {

            errorDetails = "General exception saving records in the database: " + e.toString();

            if (transaction != null) {
                transaction.rollback();
            }
        } finally {
            closeSession(tempSession);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     * Save a list of entity records while flushing a batch of records at a time
     * (to release memory)
     *
     * @param <BaseEntity>
     * @param entityList to save
     * @return
     * @throws com.library.customexception.MyCustomException
     */
    public <BaseEntity> boolean saveBulk(Set<BaseEntity> entityList) throws MyCustomException {

        int insertCount = 0;

        Session session = getSession();
        Transaction transaction = null;
        String errorDetails;

        try {

            transaction = session.beginTransaction();
            for (BaseEntity entity : entityList) {

                session.save(entity);

                if ((insertCount % NamedConstants.HIBERNATE_JDBC_BATCH) == 0) { // Same as the JDBC batch size
                    //flush a batch of inserts and release memory: Without the call to the flush method,
                    //your first-level cache would throw an OutOfMemoryException
                    session.flush();
                    session.clear();
                }

                insertCount++;
            }

            transaction.commit();
            return Boolean.TRUE;

        } catch (HibernateException he) {

            LOGGER.error(he.toString());

            errorDetails = "hibernate exception saving records in the database: " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            LOGGER.error(e.toString());

            errorDetails = "hibernate exception saving records in the database: " + e.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {

            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     *
     * @param namedQuery
     * @param parameterName
     * @param parameterValue
     */
    public void deleteRecords(String namedQuery, String parameterName, Object parameterValue) throws MyCustomException {

        Session session = getSession();
        Transaction transaction = null;
        String errorDetails;

        String queryString = "";
        try {

            // Query query = session.createQuery("from Stock where stockCode = :code ");
            //query.setParameter("code", "7277");
            //List list = query.list();
            transaction = session.beginTransaction();
            Query query = session.getNamedQuery(namedQuery);

            queryString = query.getQueryString();

            LOGGER.debug("Parameter Name: " + parameterName + ", and value: " + parameterValue);

            switch (parameterName) {

                case "displayDate":
                    //LocalDate userId = DateUtils.convertStringToLocalDate((String) parameterValue, NamedConstants.DATE_DASH_FORMAT);
                    LocalDate date = new LocalDate(parameterValue);
                    query.setParameter(parameterName, date);

                    break;

                case "id":
                    long val = GeneralUtils.convertObjectToLong(parameterValue);
                    query.setParameter(parameterName, val);
                    break;

                default:
                    query.setParameter(parameterName, parameterValue);
                    break;
            }

            query.executeUpdate();

            transaction.commit();

            return;

        } catch (HibernateException he) {

            errorDetails = "HibernateException occurred trying to delete records with query: " + queryString + " - " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            errorDetails = "General exception occurred trying to delete records with query: " + queryString + " - " + e.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     * Fetch entities (AdText, AdResources, AdPrograms) from the database using
     * named queries
     *
     * @param <BaseEntity>
     * @param namedQuery
     * @param propertyNameValues
     *
     * @return
     */
    public <BaseEntity> Set<BaseEntity> fetchEntities(String namedQuery, Map<String, Object> propertyNameValues) throws MyCustomException {

        String errorDetails;

        Session session = getSession();
        Transaction transaction = null;

        String queryString = "";

        try {

            // Query query = session.createQuery("from Stock where stockCode = :code ");
            //query.setParameter("code", "7277");
            //List list = query.list();
            transaction = session.beginTransaction();

            Query query = session.getNamedQuery(namedQuery);

            queryString = query.getQueryString();

            propertyNameValues.entrySet().stream().forEach((entry) -> {

                String name = entry.getKey();
                Set<Object> values = (Set<Object>) entry.getValue();

                LOGGER.debug("Field Name  : " + name);
                LOGGER.debug("Field values: " + values);

                switch (name) {

                    case "screenIds":
                        Set<String> screenCodes = new HashSet<>();
                        for (Object object : values) {

                            screenCodes.add((String) object);
                        }
                        query.setParameterList(name, screenCodes);
                        break;

                    case "ispreferred":
                        Set<Boolean> preferred = new HashSet<>();
                        for (Object object : values) {

                            preferred.add((Boolean) object);
                        }
                        query.setParameterList(name, preferred);
                        break;

                    case "campaignId":
                        Set<Integer> campaignIds = new HashSet<>();
                        for (Object object : values) {

                            campaignIds.add((Integer) object);
                        }
                        query.setParameterList(name, campaignIds);
                        break;

                    case "userId":
                        Set<String> userId = new HashSet<>();
                        for (Object object : values) {

                            userId.add(String.valueOf(object));
                        }
                        query.setParameterList(name, userId);
                        break;

                    case "displayDate":
                        Set<LocalDate> displayDates = new HashSet<>();
                        for (Object object : values) {

                            LocalDate date = new LocalDate(object);
                            displayDates.add(date);
                        }
                        query.setParameterList(name, displayDates);
                        break;

                    case "id":
                        Set<Long> ids = new HashSet<>();
                        for (Object object : values) {

                            long val = GeneralUtils.convertObjectToLong(object);
                            ids.add(val);
                        }
                        query.setParameterList(name, ids);
                        break;

                    case "isUploadedToDSM": {
                        Set<Boolean> vals = new HashSet<>();
                        for (Object object : values) {

                            boolean val = (Boolean) object;
                            vals.add(val);
                        }
                        query.setParameterList(name, vals);
                        break;
                    }
                    case "fetchStatus": {
                        Set<FetchStatus> statuses = new HashSet<>();
                        for (Object object : values) {

                            FetchStatus val = FetchStatus.convertToEnum((String) object);
                            statuses.add(val);
                        }
                        query.setParameterList(name, statuses);
                        break;
                    }
                    default:
                        query.setParameterList(name, values);
                        break;
                }

            });

            Set<BaseEntity> results = new HashSet<>(query.list());

            transaction.commit();

            return results;

        } catch (HibernateException he) {

            errorDetails = "HibernateException occurred trying to execute query: " + queryString + " - " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception ex) {

            errorDetails = "General exception occurred trying to execute query: " + queryString + " - " + ex.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;

    }

    /**
     *
     * @param <BaseEntity>
     * @param namedQuery
     * @param parameterName
     * @param parameterValue
     * @return
     */
    public <BaseEntity> Set<BaseEntity> fetchEntities(String namedQuery, String parameterName, Object parameterValue) throws MyCustomException {

        String errorDetails;
        Session session = getSession();
        Transaction transaction = null;

        String queryString = "";

        try {

            // Query query = session.createQuery("from Stock where stockCode = :code ");
            //query.setParameter("code", "7277");
            //List list = query.list();
            transaction = session.beginTransaction();
            Query query = session.getNamedQuery(namedQuery);

            queryString = query.getQueryString();

            LOGGER.debug("Parameter Name : " + parameterName);
            LOGGER.debug("Parameter value: " + parameterValue);

            if (parameterValue instanceof Set) {
                query.setParameterList(parameterName, (Collection) parameterValue);
            } else {
                query.setParameter(parameterName, parameterValue);
            }

//            switch (parameterName) {
//
//                case "campaignId":
//                    int campaignId = (int) parameterValue;
//                    query.setParameter(parameterName, campaignId);
//                    break;
//
//                case "displayDate":
//                    LocalDate date = new LocalDate(parameterValue);
//                    query.setParameter(parameterName, date);
//                    break;
//
//                case "id":
//                    long id = GeneralUtils.convertObjectToLong(parameterValue);
//                    query.setParameter(parameterName, id);
//                    break;
//
//                case "userId":
//                    String userId = String.valueOf(parameterValue);
//                    query.setParameter(parameterName, userId);
//                    break;
//
//                default:
//                    query.setParameter(parameterName, parameterValue);
//                    break;
//            }
            Set<BaseEntity> results = new HashSet<>(query.list());

            transaction.commit();

            return results;

        } catch (HibernateException he) {

            he.printStackTrace();

            errorDetails = "HibernateException occurred trying to execute query: " + queryString + " - " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (NullPointerException ex) {

            errorDetails = "NullPointerException occurred trying to execute query: " + queryString + " - " + ex.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception ex) {

            errorDetails = "General exception occurred trying to execute query: " + queryString + " - " + ex.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;

    }

    /**
     *
     * @param <BaseEntity>
     * @param namedQuery
     * @return
     * @throws MyCustomException
     */
    public <BaseEntity> Set<BaseEntity> fetchEntities(String namedQuery) throws MyCustomException {

        String errorDetails;
        Session session = getSession();
        Transaction transaction = null;

        String queryString = "";

        try {

            // Query query = session.createQuery("from Stock where stockCode = :code ");
            //query.setParameter("code", "7277");
            //List list = query.list();
            transaction = session.beginTransaction();
            Query query = session.getNamedQuery(namedQuery);

            queryString = query.getQueryString();

            Set<BaseEntity> results = new HashSet<>(query.list());

            transaction.commit();

            return results;

        } catch (HibernateException he) {

            errorDetails = "HibernateException occurred trying to execute query: " + queryString + " - " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception ex) {

            errorDetails = "General exception occurred trying to execute query: " + queryString + " - " + ex.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;

    }

    /**
     * Save an entity record to a database
     *
     * @param entity to save
     * @return Database ID of saved object
     * @throws com.library.customexception.MyCustomException
     */
    public Object saveEntity(DBInterface entity) throws MyCustomException {

        Session session = getSession();
        Transaction transaction = null;
        String errorDetails;

        try {

            transaction = session.beginTransaction();
            Object entityId = session.save(entity);
            transaction.commit();

            return entityId;

        } catch (HibernateException he) {

            errorDetails = "HibernateException occurred trying to save entity: " + entity.getClass() + " - " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception ex) {

            ex.printStackTrace();

            errorDetails = "General exception occurred trying to save entity: " + entity.getClass() + " - " + ex.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;

    }

    /**
     * Save an entity record to a database
     *
     * @param entity to save
     * @throws com.library.customexception.MyCustomException
     */
    public void saveOrUpdateEntity(DBInterface entity) throws MyCustomException {

        Session tempSession = getSession();
        Transaction transaction = null;
        String errorDetails;

        try {

            transaction = tempSession.beginTransaction();
            tempSession.saveOrUpdate(entity);
            transaction.commit();
            return;

        } catch (HibernateException he) {

            errorDetails = "HibernateException occurred trying to save/update entity: " + entity.getClass() + " - " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception ex) {

            errorDetails = "General exception occurred trying to save/update entity: " + entity.getClass() + " - " + ex.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(tempSession);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    //check this method before using it, dont we need to use flush just like in bulkSave??
    public boolean bulkUpdate(Set<DBInterface> dbObjectList) throws MyCustomException {

        StatelessSession tempSession = getStatelessSession();
        Transaction transaction = null;
        String errorDetails;

        try {

            transaction = tempSession.beginTransaction();

            //check this method before using it, dont we need to use flush just like in bulkSave??
            for (DBInterface dbObject : dbObjectList) {
                tempSession.update(dbObject);
            }
            transaction.commit();
            return Boolean.TRUE;

        } catch (HibernateException he) {

            errorDetails = "HibernateException occurred trying to do a bulk update: " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception ex) {

            errorDetails = "General exception occurred while trying to do a bulk update: " + ex.toString();

            if (transaction != null) {
                transaction.rollback();
            }
        } finally {
            closeSession(tempSession);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;

    }

    public boolean updateBulk(Set<BaseEntity> entityList) throws MyCustomException {

        int updateCount = 0;

        Session tempSession = getSession();
        Transaction transaction = null;
        String errorDetails;

        try {

            transaction = tempSession.beginTransaction();
            for (BaseEntity entity : entityList) {

                tempSession.update(entity);

                if ((updateCount % NamedConstants.HIBERNATE_JDBC_BATCH) == 0) { // Same as the JDBC batch size
                    //flush a batch of inserts and release memory: Without the call to the flush method,
                    //your first-level cache would throw an OutOfMemoryException
                    tempSession.flush();
                    tempSession.clear();
                }

                updateCount++;
            }

            transaction.commit();
            return Boolean.TRUE;

        } catch (HibernateException he) {

            errorDetails = "HibernateException occurred trying to do a bulk update: " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception ex) {

            errorDetails = "General exception occurred while trying to do a bulk update: " + ex.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {

            closeSession(tempSession);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     * Update an entity in the database
     *
     * @param entity
     * @return
     * @throws com.library.customexception.MyCustomException
     */
    public boolean updateEntity(BaseEntity entity) throws MyCustomException {

        Session tempSession = getSession();
        Transaction transaction = null;
        String errorDetails;

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
            return Boolean.TRUE;

        } catch (HibernateException he) {

            errorDetails = "HibernateException occurred trying to update a record: " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception ex) {

            errorDetails = "General exception occurred trying to update a record: " + ex.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(tempSession);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     * Update an entity in the database
     *
     * @param entity
     * @return
     */
    public boolean updateEntity(DBInterface entity) throws MyCustomException {

        Session tempSession = getSession();
        Transaction transaction = null;
        String errorDetails;

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
            return Boolean.TRUE;

        } catch (HibernateException he) {

            errorDetails = "HibernateException occurred trying to update a record: " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }
        } catch (Exception ex) {

            errorDetails = "General exception occurred trying to update a record: " + ex.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(tempSession);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     * Update a Terminal Entity
     *
     * @param assignTaskId
     * @param taskTypeEnum
     * @param oldTbTerminal
     */
    public void updateTerminalEntity(long assignTaskId, TaskType taskTypeEnum, TbTerminal oldTbTerminal) throws MyCustomException {

        LOGGER.debug("Updating terminal with assignLoopTaskId: " + assignTaskId);

        StatelessSession tempSession = getStatelessSession();

        Transaction transaction;
        String errorDetails;

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

            LOGGER.debug("New Loop Task ID is            : " + assignTaskId);
            LOGGER.debug("Update Terminal Query String is: " + query.getQueryString());

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

            LOGGER.debug("Update query for Terminal with new assign loop task id executed: " + updated);

            return;
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
             .add(Restrictions.sqlRestriction("generated_id having sumOfColumn(generated_id) = ?", fileCount, intType));*/
        } catch (HibernateException he) {

            errorDetails = "hibernate exception while updating tbTerminal: " + he.toString();
        } catch (Exception e) {

            errorDetails = "General exception while updating tbTerminal: " + e.toString();

        } finally {
            closeSession(tempSession);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     *
     * @param taskTypeEnum
     * @param oldTerminalEntityList
     * @throws MyCustomException
     */
    public void bulkUpdateTerminalEntity(TaskType taskTypeEnum, Set<TbTerminal> oldTerminalEntityList) throws MyCustomException {

        StatelessSession tempSession = getStatelessSession();

        Transaction transaction;
        String errorDetails;

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
            return;

        } catch (HibernateException he) {

            errorDetails = "hibernate exception while updating tbTerminal: " + he.toString();

        } catch (Exception e) {

            errorDetails = "General exception while updating tbTerminal: " + e.toString();
        } finally {
            closeSession(tempSession);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;

    }

    /**
     *
     * @param <BaseEntity>
     * @param entityType
     * @param setPropertyName
     * @return
     */
    public <BaseEntity> Set<BaseEntity> fetchCorrespondingSet(Class entityType, String setPropertyName) throws MyCustomException {

        //StatelessSession session = getStatelessSession();
        String errorDetails;
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

            return results;

        } catch (HibernateException he) {

            errorDetails = "hibernate exception Fetching object list: " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            errorDetails = "General exception Fetching object list: " + e.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;

    }

    /**
     * Fetch records matching certain conditions
     *
     * @param <BaseEntity>
     * @param entityType
     * @return
     * @throws com.library.customexception.MyCustomException
     */
    public <BaseEntity> Set<BaseEntity> fetchBulk(Class entityType) throws MyCustomException {

        Session session = getSession();
        Transaction transaction = null;

        Set<BaseEntity> results = new HashSet<>();
        String errorDetails;

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
            transaction.commit();

            return results;

        } catch (HibernateException he) {

            errorDetails = "hibernate exception Fetching object list: " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            errorDetails = "General exception Fetching object list: " + e.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     *
     * @param entityType
     * @param propertyName
     * @param propertyValue
     * @return
     * @throws com.library.customexception.MyCustomException
     */
    public DBInterface fetchEntity(Class entityType, String propertyName, Object propertyValue) throws MyCustomException {

        Session session = getSession();
        Transaction transaction = null;
        String errorDetails;

        try {

            transaction = session.beginTransaction();
            Criteria criteria = session.createCriteria(entityType);
            criteria.add(Restrictions.eq(propertyName, propertyValue));
            criteria.setMaxResults(1);

            DBInterface result = (DBInterface) criteria.uniqueResult();

            transaction.commit();
            return result;

        } catch (HibernateException he) {

            errorDetails = "hibernate exception fetching a record from database: " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            errorDetails = "General exception Fetching a record from database: " + e.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     *
     * @param entityType
     * @param propertyNameValues
     * @return
     */
    public BaseEntity fetchEntity(Class entityType, Map<String, Set<Object>> propertyNameValues) throws MyCustomException {

        Session session = getSession();
        Transaction transaction = null;
        String errorDetails;

        try {

            transaction = session.beginTransaction();
            Criteria criteria = session.createCriteria(entityType);

            //criteria.add(Restrictions.gt("dealerId", dealerId));
            // this tells Hibernate that the makes must be fetched from the database
            // you must use the name of the annotated field in the Java class: dealerMakes
            //criteria.setFetchMode("setPropertyName", FetchMode.JOIN);
            // Hibernate will return instances of Dealer, but it will return the same instance several times
            // once per make the dealer has. To avoid this, you must use a distinct root entity transformer
            //criteria.setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
            propertyNameValues.entrySet().stream().forEach((entry) -> {

                String name = entry.getKey();
                Set<Object> values = entry.getValue();

                LOGGER.debug("Field Name  : " + name);
                LOGGER.debug("Field values: " + values);

                //if values set is empty or contains a '1' - we will select all records
                if (values == null || values.isEmpty() || values.contains(String.valueOf(1))) {

                    LOGGER.info("No Restrictions on property: " + name + ", while Fetching: " + entityType.getName() + " objects.");

                } else if (name.equals("adUsers.userId")) {

                    Set<String> userIds = new HashSet<>();
                    for (Object object : values) {

                        String userId = (String) object;
                        userIds.add(userId);
                    }
                    criteria.add(Restrictions.in(name, userIds));

                } else if (name.equals("displayDate")) {

                    Set<LocalDate> displayDates = new HashSet<>();
                    for (Object object : values) {

                        //LocalDate userId = DateUtils.convertStringToLocalDate((String) object, NamedConstants.DATE_DASH_FORMAT);
                        LocalDate date = new LocalDate(object);
                        displayDates.add(date);
                    }
                    criteria.add(Restrictions.in(name, displayDates));

                } else if (name.equals("id")) {

                    Set<Long> ids = new HashSet<>();

                    for (Object object : values) {

                        long val = GeneralUtils.convertObjectToLong(object);
                        ids.add(val);
                    }
                    criteria.add(Restrictions.in(name, ids));

                } else if (name.equals("isUploadedToDSM")) {

                    Set<Boolean> vals = new HashSet<>();

                    for (Object object : values) {

                        boolean val = (Boolean) object;
                        vals.add(val);
                    }
                    criteria.add(Restrictions.in(name, vals));

                } else {
                    criteria.add(Restrictions.in(name, values));
                }

            });

            criteria.setMaxResults(1);

            BaseEntity result = (BaseEntity) criteria.uniqueResult();
            transaction.commit();

            return result;

        } catch (HibernateException he) {

            errorDetails = "hibernate exception fetching a record from database: " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            errorDetails = "General exception Fetching a record from database: " + e.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     *
     * @param entityType
     * @param propertyNameValues
     * @return
     * @throws com.library.customexception.MyCustomException
     */
    public boolean isRecordExists(Class entityType, Map<String, Object> propertyNameValues) throws MyCustomException {

        Session session = getSession();
        Transaction transaction = null;
        String errorDetails;

        try {
            transaction = session.beginTransaction();
            Criteria criteria = session.createCriteria(entityType);

            propertyNameValues.entrySet().stream().forEach((entry) -> {

                String name = entry.getKey();
                Set<Object> values = (Set<Object>) entry.getValue();

                LOGGER.debug("Field Name  : " + name);
                LOGGER.debug("Field values: " + values);

                //if values set is empty or contains a '1' - we will select all records
                if (values == null || values.isEmpty() || values.contains(String.valueOf(1))) {

                    LOGGER.info("No Restrictions on property: " + name + ", while Fetching: " + entityType.getName() + " objects.");

                } else if (name.equals("displayDate")) {

                    Set<LocalDate> displayDates = new HashSet<>();
                    for (Object object : values) {

                        //LocalDate userId = DateUtils.convertStringToLocalDate((String) object, NamedConstants.DATE_DASH_FORMAT);
                        LocalDate date = new LocalDate(object);
                        displayDates.add(date);
                    }
                    criteria.add(Restrictions.in(name, displayDates));

                } else if (name.equals("id")) {

                    Set<Long> ids = new HashSet<>();

                    for (Object object : values) {

                        long val = GeneralUtils.convertObjectToLong(object);
                        ids.add(val);
                    }
                    criteria.add(Restrictions.in(name, ids));

                } else if (name.equals("id.fileId")) {

                    Set<Long> fileIds = new HashSet<>();

                    for (Object object : values) {

                        long val = GeneralUtils.convertObjectToLong(object);
                        fileIds.add(val);
                    }
                    criteria.add(Restrictions.in(name, fileIds));

                } else if (name.equals("isUploadedToDSM")) {

                    Set<Boolean> vals = new HashSet<>();

                    for (Object object : values) {

                        boolean val = (Boolean) object;
                        vals.add(val);
                    }
                    criteria.add(Restrictions.in(name, vals));

                } else if (name.equals("id.taskId")) {

                    Set<Integer> vals = new HashSet<>();

                    for (Object object : values) {

                        int val = (Integer) object;
                        vals.add(val);
                    }
                    criteria.add(Restrictions.in(name, vals));

                } else if (name.equals("id.cstmId")) {

                    Set<Integer> vals = new HashSet<>();

                    for (Object object : values) {

                        int val = (Integer) object;
                        vals.add(val);
                    }
                    criteria.add(Restrictions.in(name, vals));

                } else if (name.equals("cstmId")) {

                    Set<Integer> vals = new HashSet<>();

                    for (Object object : values) {

                        int val = (Integer) object;
                        vals.add(val);
                    }
                    criteria.add(Restrictions.in(name, vals));

                } else {
                    criteria.add(Restrictions.in(name, values));
                }

            });

            criteria.setProjection(Projections.rowCount());
            long count = (Long) criteria.uniqueResult();

            transaction.commit();

            LOGGER.debug("Records count is: " + count);

            if (count != 0) {
                return Boolean.TRUE;
            }

            return Boolean.FALSE;

        } catch (HibernateException he) {

            errorDetails = "HibernateException checking if record exists in database: " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            errorDetails = "General exception checking if record exists in database: " + e.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;

    }

    /**
     * Get sumOfColumn of rows
     *
     * @param entityType
     * @param propertyNameValues
     * @return
     */
    public Number countRows(Class entityType, Map<String, Object> propertyNameValues) throws MyCustomException {

        Session session = getSession();
        Transaction transaction = null;
        String errorDetails;

        try {
            transaction = session.beginTransaction();
            Criteria criteria = session.createCriteria(entityType);

            if (propertyNameValues != null) {

                propertyNameValues.entrySet().stream().forEach((entry) -> {

                    String name = entry.getKey();
                    Set<Object> values = (Set<Object>) entry.getValue();

                    LOGGER.debug("Field Name  : " + name);
                    LOGGER.debug("Field values: " + values);

                    //if values set is empty or contains a '1' - we will select all records
                    if (values == null || values.isEmpty() || values.contains(String.valueOf(1))) {

                        LOGGER.info("No Restrictions on property: " + name + ", while Fetching: " + entityType.getName() + " objects.");

                    } else if (name.equals("displayDate")) {

                        Set<LocalDate> displayDates = new HashSet<>();
                        for (Object object : values) {

                            //LocalDate userId = DateUtils.convertStringToLocalDate((String) object, NamedConstants.DATE_DASH_FORMAT);
                            LocalDate date = new LocalDate(object);
                            displayDates.add(date);
                        }
                        criteria.add(Restrictions.in(name, displayDates));

                    } else if (name.equals("id")) {

                        Set<Long> ids = new HashSet<>();

                        for (Object object : values) {

                            long val = GeneralUtils.convertObjectToLong(object);
                            ids.add(val);
                        }
                        criteria.add(Restrictions.in(name, ids));

                    } else if (name.equals("isUploadedToDSM")) {

                        Set<Boolean> vals = new HashSet<>();

                        for (Object object : values) {

                            boolean val = (Boolean) object;
                            vals.add(val);
                        }
                        criteria.add(Restrictions.in(name, vals));

                    } else if (name.equals("cstmId")) {

                        Set<Integer> vals = new HashSet<>();

                        for (Object object : values) {

                            int val = (Integer) object;
                            vals.add(val);
                        }
                        criteria.add(Restrictions.in(name, vals));

                    } else {
                        criteria.add(Restrictions.in(name, values));
                    }

                });
            }

            criteria.setProjection(Projections.rowCount());
            Number count = (Number) criteria.uniqueResult();

            transaction.commit();
            return count;

        } catch (HibernateException he) {

            errorDetails = "hibernate exception trying to count records of type: " + entityType.getCanonicalName() + " -  " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            errorDetails = "General exception trying to count records of type: " + entityType.getCanonicalName() + " -  " + e.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     * Get the sum total of a summable column, a column whose values are of type
     * Number
     *
     * @param entityType
     * @param columnName
     * @param propertyNameValues
     * @return
     */
    public Number sumColumn(Class entityType, String columnName, Map<String, Object> propertyNameValues) throws MyCustomException {

        //.setProjection(Projections.sqlProjection("sum(cast(amount as signed)* direction) as amntDir", new String[] {"amntDir"} , new Type[] {Hibernate.DOUBLE}));
        //http://stackoverflow.com/questions/4624807/using-sum-in-hibernate-criteria
        Session session = getSession();
        Transaction transaction = null;
        String errorDetails;

        try {
            transaction = session.beginTransaction();
            Criteria criteria = session.createCriteria(entityType);

            if (propertyNameValues != null) {

                propertyNameValues.entrySet().stream().forEach((entry) -> {

                    String name = entry.getKey();
                    Set<Object> values = (Set<Object>) entry.getValue();

                    LOGGER.debug("Field Name  : " + name);
                    LOGGER.debug("Field values: " + values);

                    //if values set is empty or contains a '1' - we will select all records
                    if (values == null || values.isEmpty() || values.contains(String.valueOf(1))) {

                        LOGGER.info("No Restrictions on property: " + name + ", while Fetching: " + entityType.getName() + " objects.");

                    } else if (name.equals("displayDate")) {

                        Set<LocalDate> displayDates = new HashSet<>();
                        for (Object object : values) {

                            //LocalDate userId = DateUtils.convertStringToLocalDate((String) object, NamedConstants.DATE_DASH_FORMAT);
                            LocalDate date = new LocalDate(object);
                            displayDates.add(date);
                        }
                        criteria.add(Restrictions.in(name, displayDates));

                    } else if (name.equals("id")) {

                        Set<Long> ids = new HashSet<>();

                        for (Object object : values) {

                            long val = GeneralUtils.convertObjectToLong(object);
                            ids.add(val);
                        }
                        criteria.add(Restrictions.in(name, ids));

                    } else if (name.equals("isUploadedToDSM")) {

                        Set<Boolean> vals = new HashSet<>();

                        for (Object object : values) {

                            boolean val = (Boolean) object;
                            vals.add(val);
                        }
                        criteria.add(Restrictions.in(name, vals));

                    } else if (name.equals("cstmId")) {

                        Set<Integer> vals = new HashSet<>();

                        for (Object object : values) {

                            int val = (Integer) object;
                            vals.add(val);
                        }
                        criteria.add(Restrictions.in(name, vals));

                    } else {
                        criteria.add(Restrictions.in(name, values));
                    }

                });
            }

            criteria.setProjection((Projections.sum(columnName)));
            Number sumOfColumn = (Number) criteria.uniqueResult();

            transaction.commit();

            return sumOfColumn;

        } catch (HibernateException he) {

            errorDetails = "hibernate exception trying to get sum total of a summable column of entity: " + entityType.getCanonicalName() + " -  " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            errorDetails = "General exception trying to get sum total of a summable column of entity: " + entityType.getCanonicalName() + " -  " + e.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     * Fetch records matching certain conditions
     *
     * @param <BaseEntity>
     * @param entityType
     * @param propertyNameValues
     * @return
     */
    public <BaseEntity> Set<BaseEntity> fetchBulk(Class entityType, Map<String, Object> propertyNameValues) throws MyCustomException {

        Session session = getSession();
        Transaction transaction = null;

        Set<BaseEntity> results = new HashSet<>();
        String errorDetails;

        try {

            transaction = session.beginTransaction();
            Criteria criteria = session.createCriteria(entityType);
            criteria.setCacheMode(CacheMode.REFRESH);

            propertyNameValues.entrySet().stream().forEach((entry) -> {

                String name = entry.getKey();
                Set<Object> values = (Set<Object>) entry.getValue();

                LOGGER.debug("Field Name  : " + name);
                LOGGER.debug("Field values: " + values);

                //if values set is empty or contains a '1' - we will select all records
                if (values == null || values.isEmpty() || values.contains("1")) {
                    LOGGER.info("No Restrictions on property: " + name + ", while Fetching: " + entityType.getName() + " objects.");

                } else if (name.equals("displayDate")) {
                    Set<LocalDate> displayDates = new HashSet<>();
                    for (Object object : values) {

                        //LocalDate userId = DateUtils.convertStringToLocalDate((String) object, NamedConstants.DATE_DASH_FORMAT);
                        LocalDate date = new LocalDate(object);
                        displayDates.add(date);
                    }
                    criteria.add(Restrictions.in(name, displayDates));

                } else if (name.equals("id")) {

                    Set<Long> ids = new HashSet<>();

                    for (Object object : values) {

                        long val = GeneralUtils.convertObjectToLong(object);
                        ids.add(val);
                    }
                    criteria.add(Restrictions.in(name, ids));

                } else if (name.equals("isUploadedToDSM")) {

                    Set<Boolean> vals = new HashSet<>();

                    for (Object object : values) {

                        boolean val = (Boolean) object;
                        vals.add(val);
                    }
                    criteria.add(Restrictions.in(name, vals));

                } else if (name.equals("fetchStatus")) {

                    Set<FetchStatus> vals = new HashSet<>();

                    for (Object object : values) {

                        FetchStatus val = FetchStatus.convertToEnum((String) object);
                        vals.add(val);
                    }
                    criteria.add(Restrictions.in(name, vals));

                } else {
                    criteria.add(Restrictions.in(name, values));
                }

            });

//            if(!isFetchAll){
//                criteria.add(Restrictions.allEq(propertyNameValues));
//            }
            //criteria.addOrder(Order.asc(propertyName)); // To-Do -> add the other parameters, e.g. orderby, etc
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
            return results;

        } catch (HibernateException he) {

            errorDetails = "hibernate exception Fetching records from the database: " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            errorDetails = "General exception Fetching records from the database: " + e.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     *
     * @param <BaseEntity>
     * @param entityType
     * @param propertyNameValues
     * @return
     * @throws MyCustomException
     */
    public <BaseEntity> Set<BaseEntity> fetchBulk_TempSession(Class entityType, Map<String, Object[]> propertyNameValues) throws MyCustomException {

        StatelessSession session = getStatelessSession();

        Set<BaseEntity> results = new HashSet<>();
        String errorDetails;

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

            return results;

        } catch (HibernateException he) {

            errorDetails = "hibernate exception Fetching records from the database: " + he.toString();

        } catch (Exception e) {

            errorDetails = "hibernate exception Fetching records from the database: " + e.toString();

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     * fetch bulk records that have a given property value
     *
     * @param entityType
     * @param propertyName
     * @param propertyValue
     * @return bulk of records fetched
     */
    public Set<DBInterface> fetchBulk(Class<DBInterface> entityType, String propertyName, Object propertyValue) throws MyCustomException {

        StatelessSession tempSession = getStatelessSession();

        String errorDetails;
        try {

            Criteria criteria = tempSession.createCriteria(entityType);
            criteria.add(Restrictions.eq(propertyName, propertyValue));

            ScrollableResults scrollableResults = criteria.scroll(ScrollMode.FORWARD_ONLY);

            int count = 0;
            Set<DBInterface> fetchedEntities = new HashSet<>();
            while (scrollableResults.next()) {

                if ((count > 0) && (count % 10 == 0)) {
                    LOGGER.debug("Fetched " + count + " entities");
                }
                count++;
                fetchedEntities.add((DBInterface) scrollableResults.get()[0]);

            }
            return fetchedEntities;
        } catch (HibernateException he) {

            errorDetails = "hibernate exception Fetching records from the database: " + he.toString();
        } catch (Exception e) {

            errorDetails = "General exception Fetching records from the database: " + e.toString();
        } finally {
            closeSession(tempSession);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     *
     * @param entityType
     * @return
     */
    public <T> Set<T> fetchBulkStateless(Class<T> entityType) throws MyCustomException {

        StatelessSession tempSession = getStatelessSession();
        String errorDetails;

        try {

            Criteria criteria = tempSession.createCriteria(entityType);

            ScrollableResults scrollableResults = criteria.scroll(ScrollMode.FORWARD_ONLY);

            int count = 0;
            Set<T> fetchedEntities = new HashSet<>();
            while (scrollableResults.next()) {

                if ((count > 0) && (count % 10 == 0)) {
                    LOGGER.debug("Fetched " + count + " entities");
                }
                count++;
                fetchedEntities.add((T) scrollableResults.get()[0]);

            }
            return fetchedEntities;

        } catch (HibernateException he) {

            errorDetails = "hibernate exception Fetching records from the database: " + he.toString();
        } catch (Exception e) {

            errorDetails = "General exception Fetching records from the database: " + e.toString();
        } finally {
            closeSession(tempSession);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     * Fetch only a single entity/object from the database with a temp session
     *
     * @param entityType
     * @param propertyName
     * @param propertyValue
     * @return
     */
    public DBInterface fetchEntityTempSession(Class entityType, String propertyName, Object propertyValue) throws MyCustomException {

        StatelessSession tempSession = getStatelessSession();

        String errorDetails;
        try {

            Criteria criteria = tempSession.createCriteria(entityType);
            criteria.add(Restrictions.eq(propertyName, propertyValue));
            criteria.setMaxResults(1);

            return ((DBInterface) criteria.uniqueResult());

        } catch (HibernateException he) {

            errorDetails = "hibernate exception Fetching records from the database: " + he.toString();
        } catch (Exception e) {

            errorDetails = "General exception Fetching records from the database: " + e.toString();
        } finally {
            closeSession(tempSession);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     * Fetch entire column without restrictions
     *
     * @param <T>
     * @param classType
     * @param columToFetch
     * @return
     */
    public <T> List<T> fetchOnlyColumn(Class classType, String columToFetch) throws MyCustomException {

        StatelessSession tempSession = getStatelessSession();
        String errorDetails;

        try {

            //Criteria.forClass(bob.class.getName())
            Criteria criteria = tempSession.createCriteria(classType);
            criteria.setProjection(Projections.property(columToFetch));
            //criteria.add(Restrictions.gt("id", 10));
            //criteria.add(Restrictions.eq(restrictToPropertyName, restrictionValue)); //transactions should belong to the same group
            //criteria.addOrder(Order.asc(propertyName));

            ScrollableResults scrollableResults = criteria.scroll(ScrollMode.FORWARD_ONLY);

            int count = 0;
            List<T> results = new ArrayList<>();
            while (scrollableResults.next()) {
                if (++count > 0 && count % 10 == 0) {
                    LOGGER.debug("Fetched " + count + " entities");
                }
                results.add((T) scrollableResults.get()[0]);

            }
            return results;

        } catch (HibernateException he) {
            errorDetails = "hibernate exception Fetching records from the database: " + he.toString();
        } catch (Exception e) {
            errorDetails = "General exception Fetching records from the database: " + e.toString();
        } finally {
            closeSession(tempSession);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     * Get the most recent record in the database according to the condition
     * given
     *
     * @param <T>
     * @param persistentClassType
     * @param idColumn The auto-generated db column ('id') which can be used to
     * sort or count
     * @param propertyName The name of the field that has the condition for
     * fetching this record
     * @param propertyValue The condition's value
     * @return
     * @throws MyCustomException
     */
    public <T> T getMostRecentRecord(Class<T> persistentClassType, String idColumn, String propertyName, String propertyValue) throws MyCustomException {

        StatelessSession tempSession = getStatelessSession();

        String errorDetails;

        try {

            DetachedCriteria maxCriteria = DetachedCriteria.forClass(persistentClassType);
            maxCriteria.setProjection(Projections.max(idColumn));

            Criteria criteria = tempSession.createCriteria(persistentClassType);
            criteria.add(Property.forName(idColumn).eq(maxCriteria));
            criteria.add(Restrictions.eq(propertyName, propertyValue));

            //criteria.list();
            criteria.setMaxResults(1);

            T result = (T) criteria.uniqueResult();

            return result;

        } catch (HibernateException he) {

            he.printStackTrace();
            errorDetails = "hibernate exception fetching max row: " + he.toString();
        } catch (Exception e) {

            e.printStackTrace();
            errorDetails = "General exception fetching max row: " + e.toString();

        } finally {
            closeSession(tempSession);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    /**
     *
     * @param <BaseEntity>
     * @param entityType
     * @param columToFetch
     * @param propertyNameValues
     * @return
     * @throws MyCustomException
     */
    public <BaseEntity> Set<BaseEntity> fetchOnlyColumn(Class entityType, String columToFetch, Map<String, Object> propertyNameValues) throws MyCustomException {

        Session session = getSession();
        Transaction transaction = null;

        Set<BaseEntity> results = new HashSet<>();
        String errorDetails;
        try {

            transaction = session.beginTransaction();
            Criteria criteria = session.createCriteria(entityType);
            criteria.setProjection(Projections.property(columToFetch));
            criteria.setCacheMode(CacheMode.REFRESH);

            propertyNameValues.entrySet().stream().forEach((entry) -> {

                String name = entry.getKey();
                Set<Object> values = (Set<Object>) entry.getValue();

                LOGGER.debug("Field Name  : " + name);
                LOGGER.debug("Field values: " + values);

                //if values set is empty or contains a '1' - we will select all records
                if (values == null || values.isEmpty() || values.contains(String.valueOf(1))) {

                    LOGGER.info("No Restrictions on property: " + name + ", while Fetching: " + entityType.getName() + " objects.");

                } else if (name.equals("displayDate")) {

                    Set<LocalDate> displayDates = new HashSet<>();
                    for (Object object : values) {

                        //LocalDate userId = DateUtils.convertStringToLocalDate((String) object, NamedConstants.DATE_DASH_FORMAT);
                        LocalDate date = new LocalDate(object);
                        displayDates.add(date);
                    }
                    criteria.add(Restrictions.in(name, displayDates));

                } else if (name.equals("id")) {

                    Set<Long> ids = new HashSet<>();

                    for (Object object : values) {

                        long val = GeneralUtils.convertObjectToLong(object);
                        ids.add(val);
                    }
                    criteria.add(Restrictions.in(name, ids));

                } else if (name.equals("isUploadedToDSM")) {

                    Set<Boolean> vals = new HashSet<>();

                    for (Object object : values) {

                        boolean val = (Boolean) object;
                        vals.add(val);
                    }
                    criteria.add(Restrictions.in(name, vals));

                } else {
                    criteria.add(Restrictions.in(name, values));
                }

            });

//            if(!isFetchAll){
//                criteria.add(Restrictions.allEq(propertyNameValues));
//            }
            //criteria.addOrder(Order.asc(propertyName)); // To-Do -> add the other parameters, e.g. orderby, etc
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
            return results;

        } catch (HibernateException he) {

            errorDetails = "HibernateException Fetching records from the database: " + he.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } catch (Exception e) {

            errorDetails = "General exception Fetching records from the database: " + e.toString();

            if (transaction != null) {
                transaction.rollback();
            }

        } finally {
            closeSession(session);
        }

        MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, NamedConstants.GENERIC_DB_ERR_DESC, errorDetails);
        throw error;
    }

    private static final class ConfigureHibernate {

        private SessionFactory sessionFactory;

        private ConfigureHibernate() {

            try {
                configure();
            } catch (NamingException ex) {
                LOGGER.error("Naming exception during hibernate configuration: " + ex.toString());
            } catch (HibernateException ex) {
                LOGGER.error("Hibernate exception during hibernate configuration: " + ex.toString());
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
                    LOGGER.error("Naming exception during hibernate configuration: " + ex.toString());
                } catch (HibernateException ex) {
                    LOGGER.error("Hibernate exception during hibernate configuration: " + ex.toString());
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
