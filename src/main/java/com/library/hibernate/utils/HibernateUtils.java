/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.library.hibernate.utils;

import com.library.customexception.MyCustomException;
import com.library.datamodel.Constants.NamedConstants;
import com.library.datamodel.dsm_bridge.TbCustomer;
import com.library.datamodel.dsm_bridge.TbFile;
import com.library.datamodel.dsm_bridge.TbFileId;
import com.library.datamodel.model.v1_0.AdScreen;
import com.library.hibernate.CustomHibernate;
import com.library.sglogger.util.LoggerUtil;
import com.library.utilities.GeneralUtils;
import com.library.utilities.dsmbridge.IDCreator;
import static com.library.utilities.GeneralUtils.convertListToSet;
import com.library.utilities.NumericIDGenerator;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class contains all the helper methods when dealing with Hibernate
 *
 * @author smallgod
 */
public class HibernateUtils {

    private static final LoggerUtil LOG = new LoggerUtil(HibernateUtils.class);

    /**
     * Generate the FileID To-Do -> Method fetches entire file list for each
     * call, we need to come up with a better way of doing this.
     *
     * @param customHibernate
     * @return
     * @throws com.library.customexception.MyCustomException
     */
    public static synchronized long generateLongIDOld(CustomHibernate customHibernate) throws MyCustomException {

        List<TbFileId> fileIdList = customHibernate.fetchOnlyColumn(TbFile.class, "FILE_ID");

        LOG.debug("Records fetched size [TbFile.class] : " + fileIdList.size());

        Set<Long> fileIds = new HashSet<>();
        for (TbFileId id : fileIdList) {
            LOG.debug("file id found: " + id.getFileId());
            fileIds.add(id.getFileId());
        }

        long fileID;

        do {
            fileID = IDCreator.GenerateLong();
        } while (fileIds.contains(fileID));

        return fileID;
    }

    /**
     * Generate the FileID To-Do -> Method fetches entire file list for each
     * call, we need to come up with a better way of doing this.
     *
     * @param customHibernate
     * @return
     * @throws com.library.customexception.MyCustomException
     */
    public static synchronized long generateLongIDOLD2(CustomHibernate customHibernate) throws MyCustomException {

        List<Long> fileIdList = customHibernate.fetchOnlyColumn(TbFile.class, "id.fileId");

        LOG.debug("Records fetched size [TbFile.class]: " + fileIdList.size());

        Set<Long> fileIds = GeneralUtils.convertListToSet(fileIdList);

        long fileID;

        do {
            fileID = IDCreator.GenerateLong();
        } while (fileIds.contains(fileID));

        return fileID;
    }

    /**
     * Generate the FileID To-Do -> Method fetches entire file list for each
     * call, we need to come up with a better way of doing this.
     *
     * @param customHibernate
     * @param classType
     * @param idColumnName
     * @return
     * @throws com.library.customexception.MyCustomException
     */
    public static synchronized long generateLongID(CustomHibernate customHibernate, Class classType, String idColumnName) throws MyCustomException {

        List<Long> fileIdList = customHibernate.fetchOnlyColumn(classType, idColumnName);
        //List<Long> fileIdList = customHibernate.fetchOnlyColumn(TbFile.class, "id.fileId");

        LOG.debug("Records fetched size [TbFile.class]: " + fileIdList.size());

        Set<Long> fileIds = GeneralUtils.convertListToSet(fileIdList);

        long fileID;

        do {
            fileID = IDCreator.GenerateLong();
        } while (fileIds.contains(fileID));

        return fileID;
    }

    /**
     * Generate a customer ID
     *
     * @param customHibernate
     * @return
     * @throws com.library.customexception.MyCustomException
     */
    public static synchronized int generateIntegerIDOLD2(CustomHibernate customHibernate) throws MyCustomException {

        //List<Integer> idList = customHibernate.fetchOnlyColumn(TbCustomer.class, "CSTM_ID");
        List<Integer> fileIdList = customHibernate.fetchOnlyColumn(TbCustomer.class, "cstmId");

        Set<Integer> set = convertListToSet(fileIdList);

        int customerID;

        do {
            customerID = IDCreator.GenerateInt();
        } while (set.contains(customerID));

        return customerID;
    }

    /**
     * Generate a customer ID
     *
     * @param customHibernate
     * @param classType
     * @param idColumnName
     * @return
     * @throws com.library.customexception.MyCustomException
     */
    public static synchronized int generateIntegerID(CustomHibernate customHibernate, Class classType, String idColumnName) throws MyCustomException {

        List<Integer> idList = customHibernate.fetchOnlyColumn(classType, idColumnName);

        Set<Integer> set = convertListToSet(idList);

        int generatedId;

        do {
            generatedId = IDCreator.GenerateInt();
        } while (set.contains(generatedId));

        return generatedId;
    }

    /**
     * Generate Screen Id
     *
     * @param customHibernate
     * @param businessId
     * @return
     * @throws MyCustomException
     */
    public static synchronized String generateScreenId(CustomHibernate customHibernate, String businessId) throws MyCustomException {

        //get the most recent Screen under this business
        AdScreen recentScreen = customHibernate.getMostRecentRecord(AdScreen.class, "id", "adBusiness.businessId", businessId);
        String incremStart;

        if (recentScreen == null) {
            LOG.debug("recentScreen is null");

            incremStart = NamedConstants.SCREEN_START_ID;
        } else {
            String idToIncrement = recentScreen.getScreenId();
            int len = idToIncrement.length();
            idToIncrement = idToIncrement.substring(len - 2); //e.g. if screenId is SASS01, substring will give us -> '01'

            //generatorId = AlphaNumericIDGenerator.generateNextId(idToIncrement);
            incremStart = NumericIDGenerator.generateNextId(idToIncrement);
        }

        String screenId = businessId + "-" + incremStart;

        return screenId;

    }

}
