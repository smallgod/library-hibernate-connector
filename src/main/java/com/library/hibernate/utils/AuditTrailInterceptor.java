/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.library.hibernate.utils;

/**
 *
 * @author smallgod
 */
import com.library.datamodel.Constants.NamedConstants;
import com.library.datamodel.model.v1_0.AdArea;
import com.library.datamodel.model.v1_0.AdAudienceType;
import com.library.datamodel.model.v1_0.AdBusinessType;
import com.library.datamodel.model.v1_0.AdClient;
import com.library.datamodel.model.v1_0.AdMonitor;
import com.library.datamodel.model.v1_0.AdPayment;
import com.library.datamodel.model.v1_0.AdProgram;
import com.library.datamodel.model.v1_0.AdResource;
import com.library.datamodel.model.v1_0.AdSchedule;
import com.library.datamodel.model.v1_0.AdScreen;
import com.library.datamodel.model.v1_0.AdScreenOwner;
import com.library.datamodel.model.v1_0.AdTerminal;
import com.library.sgsharedinterface.Auditable;
import com.library.sglogger.util.LoggerUtil;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.hibernate.EmptyInterceptor;
import org.hibernate.type.Type;
import org.joda.time.LocalDateTime;

public class AuditTrailInterceptor extends EmptyInterceptor {

    private static final LoggerUtil logger = new LoggerUtil(AuditTrailInterceptor.class);
    private static final long serialVersionUID = 5997616111315960747L;

    public AuditTrailInterceptor() {
    }

    @Override
    public void onDelete(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) {
        logger.debug("Delete event");
    }

    @Override
    public boolean onLoad(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) {

        logger.debug("onLoad event");
        return true;
    }

    @Override
    public boolean onSave(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) {

        logger.debug("onSave called");

        if (entity instanceof Auditable) {
            setValue(state, propertyNames, NamedConstants.PROPNAME_CREATED_BY, ((Auditable) entity).getUsername());
            setValue(state, propertyNames, NamedConstants.PROPNAME_CREATED_ON, new LocalDateTime());

            return true;
        }

        return false;
    }

    /**
     * called before commit into database
     *
     * @param entities
     */
    @Override
    public void preFlush(Iterator entities) {
        logger.debug("preFlush operation, b4 commiting to db  >> preFlush event");
        logger.debug("postFlush: List of objects that have been flushed... ");
        int i = 0;
        while (entities.hasNext()) {
            Object entity = entities.next();

            if (entity instanceof AdProgram) {
                AdProgram adProgram = (AdProgram) entity;
                logger.debug("preFlush().. about to commit an instance of AdProgram: " + adProgram.getId());

            } else if (entity instanceof AdScreen) {
                AdScreen adScreen = (AdScreen) entity;
                logger.debug("preFlush().. about to commit an instance of AdScreen: " + adScreen.getId());

            } else if (entity instanceof AdResource) {
                AdResource adResource = (AdResource) entity;
                logger.debug("preFlush().. about to commit an instance of Advert: " + adResource.getId());

            } else if (entity instanceof AdPayment) {
                AdPayment adPayment = (AdPayment) entity;
                logger.debug("preFlush().. about to commit an instance of AdResource: " + adPayment.getId());

            } else if (entity instanceof AdClient) {
                AdClient adClient = (AdClient) entity;
                logger.debug("preFlush().. about to commit an instance of AdClient: " + adClient.getId());

            } else if (entity instanceof AdScreenOwner) {
                AdScreenOwner adScreenOwner = (AdScreenOwner) entity;
                logger.debug("preFlush().. about to commit an instance of AdScreenOwner: " + adScreenOwner.getId());

            } else if (entity instanceof AdTerminal) {
                AdTerminal adTerminal = (AdTerminal) entity;
                logger.debug("preFlush().. about to commit an instance of AdTerminal: " + adTerminal.getId());

            } else if (entity instanceof AdSchedule) {
                AdSchedule adSchedule = (AdSchedule) entity;
                logger.debug("preFlush().. about to commit an instance of AdSchedule: " + adSchedule.getId());

            } else if (entity instanceof AdArea) {
                AdArea area = (AdArea) entity;
                logger.debug("preFlush().. about to commit an instance of AdScreenArea: " + area.getId());

            } else if (entity instanceof AdMonitor) {
                AdMonitor monitor = (AdMonitor) entity;
                logger.debug("preFlush().. about to commit an instance of AdMonitor: " + monitor.getId());

            } else if (entity instanceof AdBusinessType) {
                AdBusinessType locationType = (AdBusinessType) entity;
                logger.debug("preFlush().. about to commit an instance of LocationType: " + locationType.getId());

            } else if (entity instanceof AdAudienceType) {
                AdAudienceType audienceType = (AdAudienceType) entity;
                logger.debug("preFlush().. about to commit an instance of AudienceType: " + audienceType.getId());

            }//To-DO add other entities

            logger.debug("postFlush: " + (++i) + " : " + entity);
        }
    }

    /**
     * Called after committed into database This method is called after a flush
     * has occurred and an object has been updated in memory
     *
     * @param entities
     */
    @Override
    public void postFlush(Iterator entities) {

        logger.debug("postFlush operation, after commiting to db  >> postFlush event");
        logger.debug("preFlush: List of objects to flush... ");

        int i = 0;
        while (entities.hasNext()) {

            Object entity = entities.next();

            if (entity instanceof AdProgram) {
                AdProgram adProgram = (AdProgram) entity;
                logger.debug("postFlush().. about to commit an instance of AdProgram: " + adProgram.getId());

            } else if (entity instanceof AdScreen) {
                AdScreen adScreen = (AdScreen) entity;
                logger.debug("postFlush().. about to commit an instance of AdScreen: " + adScreen.getId());

            } else if (entity instanceof AdResource) {
                AdResource adResource = (AdResource) entity;
                logger.debug("postFlush().. about to commit an instance of Advert: " + adResource.getId());

            } else if (entity instanceof AdPayment) {
                AdPayment adPayment = (AdPayment) entity;
                logger.debug("postFlush().. about to commit an instance of AdResource: " + adPayment.getId());

            } else if (entity instanceof AdClient) {
                AdClient adClient = (AdClient) entity;
                logger.debug("postFlush().. about to commit an instance of AdClient: " + adClient.getId());

            } else if (entity instanceof AdScreenOwner) {
                AdScreenOwner adScreenOwner = (AdScreenOwner) entity;
                logger.debug("preFlush().. about to commit an instance of AdScreenOwner: " + adScreenOwner.getId());

            } else if (entity instanceof AdTerminal) {
                AdTerminal adTerminal = (AdTerminal) entity;
                logger.debug("postFlush().. about to commit an instance of AdTerminal: " + adTerminal.getId());

            } else if (entity instanceof AdSchedule) {
                AdSchedule adSchedule = (AdSchedule) entity;
                logger.debug("postFlush().. about to commit an instance of AdSchedule: " + adSchedule.getId());

            } else if (entity instanceof AdArea) {
                AdArea area = (AdArea) entity;
                logger.debug("postFlush().. about to commit an instance of AdScreenArea: " + area.getId());

            } else if (entity instanceof AdMonitor) {
                AdMonitor monitor = (AdMonitor) entity;
                logger.debug("postFlush().. about to commit an instance of AdMonitor: " + monitor.getId());

            } else if (entity instanceof AdBusinessType) {
                AdBusinessType locationType = (AdBusinessType) entity;
                logger.debug("postFlush().. about to commit an instance of LocationType: " + locationType.getId());

            } else if (entity instanceof AdAudienceType) {
                AdAudienceType audienceType = (AdAudienceType) entity;
                logger.debug("postFlush().. about to commit an instance of AudienceType: " + audienceType.getId());

            }//To-DO add other entities

            logger.info("preFlush: " + (++i) + " : " + entity);
        }
    }

    @Override
    public boolean onFlushDirty(Object entity, Serializable id, Object[] currentState, Object[] previousState, String[] propertyNames, Type[] types) {

        logger.debug("Update Operation >> onFlushDirty event");

        if (entity instanceof Auditable) {

            logger.debug("this is where we add some audit trail but for now leave it out till we get a proper way to deal with the 2 ever increasing string length");

            
            setValue(currentState, propertyNames, NamedConstants.PROPNAME_LAST_MODIFIED_BY, ((Auditable) entity).getUsername());
            setValue(currentState, propertyNames, NamedConstants.PROPNAME_DATE_LAST_MODIFIED, new LocalDateTime());
            
            /*
            updateValue(currentState, propertyNames, NamedConstants.PROPNAME_DATE_MODIFIED_HISTORY, ((Auditable) entity).getUsername());
            updateValue(currentState, propertyNames, NamedConstants.PROPNAME_MODIFIED_BY_HISTORY, ((Auditable) entity).getUsername());
             */
            return true;
        }

        return false;
    }

    /**
     * Set a completely new value for a property of an auditable entity
     *
     * @param currentState
     * @param propertyNames
     * @param propertyToSet
     * @param value
     */
    private void setValue(Object[] currentState, String[] propertyNames, String propertyToSet, Object value) {

        int index = Arrays.asList(propertyNames).indexOf(propertyToSet);

        if (index >= 0) {
            currentState[index] = value;
        }
    }

    /**
     * Update (add delimeter and then new value) the property of an auditable
     * entity
     *
     * @param currentState
     * @param propertyNames
     * @param propertyToSet
     * @param value
     */
    private void updateValue(Object[] currentState, String[] propertyNames, String propertyToSet, Object value) {

        int index = Arrays.asList(propertyNames).indexOf(propertyToSet);

        if (index >= 0) {

            Object obj = currentState[index];
            String strObj = "";

            if (obj instanceof String) {
                strObj = (String) obj;
            }

            currentState[index] = (strObj + "|" + value);

            logger.debug("val: " + currentState[index]);
        }
    }
}
