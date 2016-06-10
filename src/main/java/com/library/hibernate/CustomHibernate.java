package com.library.hibernate;

import com.library.datamodel.Constants.NamedConstants;
import com.library.datamodel.model.v1_0.BaseModel;
import com.library.hibernate.utils.AuditTrailInterceptor;
import com.library.hibernate.utils.CallBack;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.naming.NamingException;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Restrictions;
import org.hibernate.service.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author smallgod
 */
public final class CustomHibernate {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomHibernate.class);
    private static String hibernateFilePath;
    private final SessionFactory sessionFactory;

    public CustomHibernate(String hibernateFilePath) {
        CustomHibernate.hibernateFilePath = hibernateFilePath;
        sessionFactory = ConfigureHibernate.getInstance().getSessionFactory();
    }

    private SessionFactory getSessionFactory() {

        return sessionFactory;
    }

    /**
     * Close the hibernate session factory after use
     */
    public void releaseDBResources() {

        if (getSessionFactory() != null && !getSessionFactory().isClosed()) {
            getSessionFactory().close();

            LOGGER.debug("Closing Hibernate SessionFactory...");
        } else{
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
    public boolean insertBulk(List<BaseModel> entityList) {

        StatelessSession tempSession = getStatelessSession();
        Transaction transaction = null;
        boolean committed = false;

        try {

            transaction = tempSession.beginTransaction();
            for (BaseModel entity : entityList) {
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
    public boolean saveBulk(List<BaseModel> entityList) {

        int insertCount = 0;

        Session tempSession = getSession();
        Transaction transaction = null;
        boolean committed = false;

        try {

            transaction = tempSession.beginTransaction();
            for (BaseModel entity : entityList) {

                tempSession.save(entity);

                if ((insertCount % NamedConstants.HIBERNATE_JDBC_BATCH) == 0) { // Same as the JDBC batch size
                    //flush a batch of inserts and release memory:
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
     * Save an entity record to a database
     *
     * @param entity to save
     * @return Database ID of saved object
     */
    public long saveEntity(BaseModel entity) {

        long entityId = 0;
        Session tempSession = getSession();
        Transaction transaction = null;

        try {

            transaction = tempSession.beginTransaction();
            entityId = (Long) tempSession.save(entity);
            transaction.commit();

        } catch (HibernateException he) {
            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("Error saving entity: " + he.getMessage());
            //throw new MyCustomException("Hibernate Error saving object to DB", ErrorCode.PROCESSING_ERR, "Error saving: " + he.getMessage(), ErrorCategory.SERVER_ERR_TYPE);

        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
            }
            LOGGER.error("Error saving entity: " + e.getMessage());
            //throw new MyCustomException("Error saving object to DB", ErrorCode.PROCESSING_ERR, "Error saving: " + e.getMessage(), ErrorCategory.SERVER_ERR_TYPE);

        } finally {
            closeSession(tempSession);
        }

        return entityId;
    }

    /**
     * fetch bulk records that have a given property value
     *
     * @param entityType
     * @param propertyName
     * @param propertyValue
     * @return bulk of records fetched
     */
    public Set<BaseModel> fetchBulk(Class<BaseModel> entityType, String propertyName, Object propertyValue) {

        StatelessSession tempSession = getStatelessSession();
        Set<BaseModel> fetchedEntities = new HashSet<>();

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
                fetchedEntities.add((BaseModel) scrollableResults.get()[0]);

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

        private SessionFactory getSessionFactory() {

            if (sessionFactory == null || sessionFactory.isClosed()) {

                LOGGER.debug("SessionFactory is NULL or closed going to reconfigure");

                try {
                    configure();
                } catch (NamingException ex) {
                    LOGGER.error("Naming exception during hibernate configuration: " + ex.getMessage());
                } catch (HibernateException ex) {
                    LOGGER.error("Hibernate exception during hibernate configuration: " + ex.getMessage());
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

            LOGGER.warn(">>>>>>>> configure() method called here... IT IS HAPPENING, TAKE NOTE!!!!!!!");

            File file = new File(hibernateFilePath);

            Configuration configuration = new Configuration();
            configuration.configure(file);
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
