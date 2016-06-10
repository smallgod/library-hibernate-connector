/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.library.hibernate.utils;

import org.hibernate.StatelessSession;

/**
 *
 * @author smallgod
 */
public interface CallBack {

    public void execute(Object data);

    public int processAndSaveMultipleRecords(StatelessSession tempSession);
}
