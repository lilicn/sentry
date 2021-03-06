/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.db.service.persistent;

import static org.apache.sentry.core.common.utils.SentryConstants.AUTHORIZABLE_JOINER;
import static org.apache.sentry.core.common.utils.SentryConstants.KV_JOINER;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jdo.FetchGroup;
import javax.jdo.JDODataStoreException;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.common.exception.SentrySiteConfigurationException;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.DBModelAuthorizable.AuthorizableType;
import org.apache.sentry.core.common.exception.SentryAccessDeniedException;
import org.apache.sentry.core.common.exception.SentryAlreadyExistsException;
import org.apache.sentry.core.common.exception.SentryGrantDeniedException;
import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.core.common.exception.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.service.model.MSentryGroup;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryUser;
import org.apache.sentry.provider.db.service.model.MSentryVersion;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyStoreProcessor;
import org.apache.sentry.provider.db.service.thrift.TSentryActiveRoleSet;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryGroup;
import org.apache.sentry.provider.db.service.thrift.TSentryMappingData;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilegeMap;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.datanucleus.store.rdbms.exceptions.MissingTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * SentryStore is the data access object for Sentry data. Strings
 * such as role and group names will be normalized to lowercase
 * in addition to starting and ending whitespace.
 */
public class SentryStore {
  private static final UUID SERVER_UUID = UUID.randomUUID();
  private static final Logger LOGGER = LoggerFactory
          .getLogger(SentryStore.class);

  public static final String NULL_COL = "__NULL__";
  public static int INDEX_GROUP_ROLES_MAP = 0;
  public static int INDEX_USER_ROLES_MAP = 1;
  static final String DEFAULT_DATA_DIR = "sentry_policy_db";

  private static final Set<String> ALL_ACTIONS = Sets.newHashSet(AccessConstants.ALL,
      AccessConstants.SELECT, AccessConstants.INSERT, AccessConstants.ALTER,
      AccessConstants.CREATE, AccessConstants.DROP, AccessConstants.INDEX,
      AccessConstants.LOCK);

  // Now partial revoke just support action with SELECT,INSERT and ALL.
  // e.g. If we REVOKE SELECT from a privilege with action ALL, it will leads to INSERT
  // Otherwise, if we revoke other privilege(e.g. ALTER,DROP...), we will remove it from a role directly.
  private static final Set<String> PARTIAL_REVOKE_ACTIONS = Sets.newHashSet(AccessConstants.ALL,
      AccessConstants.ACTION_ALL.toLowerCase(), AccessConstants.SELECT, AccessConstants.INSERT);

  /**
   * Commit order sequence id. This is used by notification handlers
   * to know the order in which events where committed to the database.
   * This instance variable is incremented in incrementGetSequenceId
   * and read in commitUpdateTransaction. Synchronization on this
   * is required to read commitSequenceId.
   */
  private long commitSequenceId;
  private final PersistenceManagerFactory pmf;
  private Configuration conf;
  private PrivCleaner privCleaner = null;
  private Thread privCleanerThread = null;
  private final TransactionManager tm;

  public SentryStore(Configuration conf) throws Exception {
    commitSequenceId = 0;
    this.conf = conf;
    Properties prop = new Properties();
    prop.putAll(ServerConfig.SENTRY_STORE_DEFAULTS);
    String jdbcUrl = conf.get(ServerConfig.SENTRY_STORE_JDBC_URL, "").trim();
    Preconditions.checkArgument(!jdbcUrl.isEmpty(), "Required parameter " +
        ServerConfig.SENTRY_STORE_JDBC_URL + " is missed");
    String user = conf.get(ServerConfig.SENTRY_STORE_JDBC_USER, ServerConfig.
        SENTRY_STORE_JDBC_USER_DEFAULT).trim();
    //Password will be read from Credential provider specified using property
    // CREDENTIAL_PROVIDER_PATH("hadoop.security.credential.provider.path" in sentry-site.xml
    // it falls back to reading directly from sentry-site.xml
    char[] passTmp = conf.getPassword(ServerConfig.SENTRY_STORE_JDBC_PASS);
    String pass = null;
    if(passTmp != null) {
      pass = new String(passTmp);
    } else {
      throw new SentrySiteConfigurationException("Error reading " + ServerConfig.SENTRY_STORE_JDBC_PASS);
    }

    String driverName = conf.get(ServerConfig.SENTRY_STORE_JDBC_DRIVER,
        ServerConfig.SENTRY_STORE_JDBC_DRIVER_DEFAULT);
    prop.setProperty(ServerConfig.JAVAX_JDO_URL, jdbcUrl);
    prop.setProperty(ServerConfig.JAVAX_JDO_USER, user);
    prop.setProperty(ServerConfig.JAVAX_JDO_PASS, pass);
    prop.setProperty(ServerConfig.JAVAX_JDO_DRIVER_NAME, driverName);
    for (Map.Entry<String, String> entry : conf) {
      String key = entry.getKey();
      if (key.startsWith(ServerConfig.SENTRY_JAVAX_JDO_PROPERTY_PREFIX) ||
          key.startsWith(ServerConfig.SENTRY_DATANUCLEUS_PROPERTY_PREFIX)) {
        key = StringUtils.removeStart(key, ServerConfig.SENTRY_DB_PROPERTY_PREFIX);
        prop.setProperty(key, entry.getValue());
      }
    }


    boolean checkSchemaVersion = conf.get(
        ServerConfig.SENTRY_VERIFY_SCHEM_VERSION,
        ServerConfig.SENTRY_VERIFY_SCHEM_VERSION_DEFAULT).equalsIgnoreCase(
            "true");
    if (!checkSchemaVersion) {
      prop.setProperty("datanucleus.schema.autoCreateAll", "true");
      prop.setProperty("datanucleus.autoCreateSchema", "true");
      prop.setProperty("datanucleus.fixedDatastore", "false");
    }

    // Disallow operations outside of transactions
    prop.setProperty("datanucleus.NontransactionalRead", "false");
    prop.setProperty("datanucleus.NontransactionalWrite", "false");

    pmf = JDOHelper.getPersistenceManagerFactory(prop);
    tm = new TransactionManager(pmf, conf);
    verifySentryStoreSchema(checkSchemaVersion);

    // Kick off the thread that cleans orphaned privileges (unless told not to)
    privCleaner = this.new PrivCleaner();
    if (conf.get(ServerConfig.SENTRY_STORE_ORPHANED_PRIVILEGE_REMOVAL,
            ServerConfig.SENTRY_STORE_ORPHANED_PRIVILEGE_REMOVAL_DEFAULT)
            .equalsIgnoreCase("true")) {
      privCleanerThread = new Thread(privCleaner);
      privCleanerThread.start();
    }
  }

  public TransactionManager getTransactionManager() {
    return tm;
  }

  // ensure that the backend DB schema is set
  public void verifySentryStoreSchema(boolean checkVersion) throws Exception {
    if (!checkVersion) {
      setSentryVersion(SentryStoreSchemaInfo.getSentryVersion(),
          "Schema version set implicitly");
    } else {
      String currentVersion = getSentryVersion();
      if (!SentryStoreSchemaInfo.getSentryVersion().equals(currentVersion)) {
        throw new SentryAccessDeniedException(
            "The Sentry store schema version " + currentVersion
            + " is different from distribution version "
            + SentryStoreSchemaInfo.getSentryVersion());
      }
    }
  }

  public synchronized void stop() {
    if (privCleanerThread != null) {
      privCleaner.exit();
      try {
        privCleanerThread.join();
      } catch (InterruptedException e) {
        // Ignore...
      }
    }
    if (pmf != null) {
      pmf.close();
    }
  }

  /**
   * Increments commitSequenceId which should not be modified outside
   * this method.
   *
   * @return sequence id
   */
  private synchronized long incrementGetSequenceId() {
    return ++commitSequenceId;
  }

  public void rollbackTransaction(PersistenceManager pm) {
    if (pm == null || pm.isClosed()) {
      return;
    }
    Transaction currentTransaction = pm.currentTransaction();
    if (currentTransaction.isActive()) {
      try {
        currentTransaction.rollback();
      } finally {
        pm.close();
      }
    }
  }
  /**
  Get the MSentry object from roleName
  Note: Should be called inside a transaction
   */
  public MSentryRole getMSentryRole(PersistenceManager pm, String roleName) {
    Query query = pm.newQuery(MSentryRole.class);
    query.setFilter("this.roleName == t");
    query.declareParameters("java.lang.String t");
    query.setUnique(true);
    return (MSentryRole) query.execute(roleName);
  }

  /**
   * Normalize the string values
   */
  private String trimAndLower(String input) {
    return input.trim().toLowerCase();
  }
  /**
   * Create a sentry role and persist it.
   * @param roleName: Name of the role being persisted
   * @returns commit context used for notification handlers
   * @throws SentryAlreadyExistsException
   */
  public CommitContext createSentryRole(final String roleName) throws Exception {
    return (CommitContext)tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            createSentryRoleCore(pm, roleName);
            return new CommitContext(SERVER_UUID, incrementGetSequenceId());
            }
        });
  }

  private void createSentryRoleCore(PersistenceManager pm, String roleName)
      throws SentryAlreadyExistsException {
    String trimmedRoleName = trimAndLower(roleName);
    MSentryRole mSentryRole = getMSentryRole(pm, trimmedRoleName);
    if (mSentryRole == null) {
      MSentryRole mRole = new MSentryRole(trimmedRoleName, System.currentTimeMillis());
      pm.makePersistent(mRole);
    } else {
      throw new SentryAlreadyExistsException("Role: " + trimmedRoleName);
    }
  }

  private <T> Long getCount(final Class<T> tClass) {
    Long size;
    try {
      size = (Long) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              Query query = pm.newQuery();
              query.setClass(tClass);
              query.setResult("count(this)");
              return (Long) query.execute();
            }
          });
    } catch (Exception e) {
      size = Long.valueOf(-1);
    }
    return size;
  }
  public Gauge<Long> getRoleCountGauge() {
    return new Gauge< Long >() {
      @Override
      public Long getValue() {
        return getCount(MSentryRole.class);
      }
    };
  }

  public Gauge<Long> getPrivilegeCountGauge() {
    return new Gauge< Long >() {
      @Override
      public Long getValue() {
        return getCount(MSentryPrivilege.class);
      }
    };
  }

  public Gauge<Long> getGroupCountGauge() {
    return new Gauge< Long >() {
      @Override
      public Long getValue() {
        return getCount(MSentryGroup.class);
      }
    };
  }

  public Gauge<Long> getUserCountGauge() {
    return new Gauge<Long>() {
      @Override
      public Long getValue() {
        return getCount(MSentryUser.class);
      }
    };
  }

  /**
   * Lets the test code know how many privs are in the db, so that we know
   * if they are in fact being cleaned up when not being referenced any more.
   * @return The number of rows in the db priv table.
   */
  @VisibleForTesting
  long countMSentryPrivileges() {
    return getCount(MSentryPrivilege.class);
  }

  @VisibleForTesting
  void clearAllTables() {
    try {
      tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              pm.newQuery(MSentryRole.class).deletePersistentAll();
              pm.newQuery(MSentryGroup.class).deletePersistentAll();
              pm.newQuery(MSentryUser.class).deletePersistentAll();
              pm.newQuery(MSentryPrivilege.class).deletePersistentAll();
              return null;
            }
          });
    } catch (Exception e) {
      // the method only for test, log the error and ignore the exception
      LOGGER.error(e.getMessage(), e);
    }
  }

  public CommitContext alterSentryRoleGrantPrivilege(String grantorPrincipal,
      String roleName, TSentryPrivilege privilege) throws Exception {
    return alterSentryRoleGrantPrivileges(grantorPrincipal,
        roleName, Sets.newHashSet(privilege));
  }

  public CommitContext alterSentryRoleGrantPrivileges(final String grantorPrincipal,
      final String roleName, final Set<TSentryPrivilege> privileges) throws Exception {
    return (CommitContext)tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            String trimmedRoleName = trimAndLower(roleName);
            for (TSentryPrivilege privilege : privileges) {
              // first do grant check
              grantOptionCheck(pm, grantorPrincipal, privilege);
              MSentryPrivilege mPrivilege = alterSentryRoleGrantPrivilegeCore(
                  pm, trimmedRoleName, privilege);
              if (mPrivilege != null) {
                convertToTSentryPrivilege(mPrivilege, privilege);
              }
            }
            return new CommitContext(SERVER_UUID, incrementGetSequenceId());
          }
        });
  }

  private MSentryPrivilege alterSentryRoleGrantPrivilegeCore(PersistenceManager pm,
      String roleName, TSentryPrivilege privilege)
      throws SentryNoSuchObjectException, SentryInvalidInputException {
    MSentryPrivilege mPrivilege = null;
    MSentryRole mRole = getMSentryRole(pm, roleName);
    if (mRole == null) {
      throw new SentryNoSuchObjectException("Role: " + roleName + " doesn't exist");
    } else {

      if (!isNULL(privilege.getColumnName()) || !isNULL(privilege.getTableName())
          || !isNULL(privilege.getDbName())) {
        // If Grant is for ALL and Either INSERT/SELECT already exists..
        // need to remove it and GRANT ALL..
        if (AccessConstants.ALL.equalsIgnoreCase(privilege.getAction())
            || AccessConstants.ACTION_ALL.equalsIgnoreCase(privilege.getAction())) {
          TSentryPrivilege tNotAll = new TSentryPrivilege(privilege);
          tNotAll.setAction(AccessConstants.SELECT);
          MSentryPrivilege mSelect = getMSentryPrivilege(tNotAll, pm);
          tNotAll.setAction(AccessConstants.INSERT);
          MSentryPrivilege mInsert = getMSentryPrivilege(tNotAll, pm);
          if (mSelect != null && mRole.getPrivileges().contains(mSelect)) {
            mSelect.removeRole(mRole);
            privCleaner.incPrivRemoval();
            pm.makePersistent(mSelect);
          }
          if (mInsert != null && mRole.getPrivileges().contains(mInsert)) {
            mInsert.removeRole(mRole);
            privCleaner.incPrivRemoval();
            pm.makePersistent(mInsert);
          }
        } else {
          // If Grant is for Either INSERT/SELECT and ALL already exists..
          // do nothing..
          TSentryPrivilege tAll = new TSentryPrivilege(privilege);
          tAll.setAction(AccessConstants.ALL);
          MSentryPrivilege mAll1 = getMSentryPrivilege(tAll, pm);
          tAll.setAction(AccessConstants.ACTION_ALL);
          MSentryPrivilege mAll2 = getMSentryPrivilege(tAll, pm);
          if (mAll1 != null && mRole.getPrivileges().contains(mAll1)) {
            return null;
          }
          if (mAll2 != null && mRole.getPrivileges().contains(mAll2)) {
            return null;
          }
        }
      }

      mPrivilege = getMSentryPrivilege(privilege, pm);
      if (mPrivilege == null) {
        mPrivilege = convertToMSentryPrivilege(privilege);
      }
      mPrivilege.appendRole(mRole);
      pm.makePersistent(mRole);
      pm.makePersistent(mPrivilege);
    }
    return mPrivilege;
  }

  public CommitContext alterSentryRoleRevokePrivilege(String grantorPrincipal,
      String roleName, TSentryPrivilege tPrivilege) throws Exception {
    return alterSentryRoleRevokePrivileges(grantorPrincipal,
        roleName, Sets.newHashSet(tPrivilege));
  }

  public CommitContext alterSentryRoleRevokePrivileges(final String grantorPrincipal,
      final String roleName, final Set<TSentryPrivilege> tPrivileges) throws Exception {
    return (CommitContext)tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            String trimmedRoleName = safeTrimLower(roleName);
            for (TSentryPrivilege tPrivilege : tPrivileges) {
              // first do revoke check
              grantOptionCheck(pm, grantorPrincipal, tPrivilege);
              alterSentryRoleRevokePrivilegeCore(pm, trimmedRoleName, tPrivilege);
            }
            return new CommitContext(SERVER_UUID, incrementGetSequenceId());
          }
        });
  }

  private void alterSentryRoleRevokePrivilegeCore(PersistenceManager pm,
      String roleName, TSentryPrivilege tPrivilege)
      throws SentryNoSuchObjectException, SentryInvalidInputException {
    Query query = pm.newQuery(MSentryRole.class);
    query.setFilter("this.roleName == t");
    query.declareParameters("java.lang.String t");
    query.setUnique(true);
    MSentryRole mRole = (MSentryRole) query.execute(roleName);
    if (mRole == null) {
      throw new SentryNoSuchObjectException("Role: " + roleName + " doesn't exist");
    } else {
      query = pm.newQuery(MSentryPrivilege.class);
      MSentryPrivilege mPrivilege = getMSentryPrivilege(tPrivilege, pm);
      if (mPrivilege == null) {
        mPrivilege = convertToMSentryPrivilege(tPrivilege);
      } else {
        mPrivilege = (MSentryPrivilege) pm.detachCopy(mPrivilege);
      }

      Set<MSentryPrivilege> privilegeGraph = Sets.newHashSet();
      if (mPrivilege.getGrantOption() != null) {
        privilegeGraph.add(mPrivilege);
      } else {
        MSentryPrivilege mTure = new MSentryPrivilege(mPrivilege);
        mTure.setGrantOption(true);
        privilegeGraph.add(mTure);
        MSentryPrivilege mFalse = new MSentryPrivilege(mPrivilege);
        mFalse.setGrantOption(false);
        privilegeGraph.add(mFalse);
      }
      // Get the privilege graph
      populateChildren(pm, Sets.newHashSet(roleName), mPrivilege, privilegeGraph);
      for (MSentryPrivilege childPriv : privilegeGraph) {
        revokePrivilegeFromRole(pm, tPrivilege, mRole, childPriv);
      }
      pm.makePersistent(mRole);
    }
  }

  /**
   * Roles can be granted ALL, SELECT, and INSERT on tables. When
   * a role has ALL and SELECT or INSERT are revoked, we need to remove the ALL
   * privilege and add SELECT (INSERT was revoked) or INSERT (SELECT was revoked).
   */
  private void revokePartial(PersistenceManager pm,
      TSentryPrivilege requestedPrivToRevoke, MSentryRole mRole,
      MSentryPrivilege currentPrivilege) throws SentryInvalidInputException {
    MSentryPrivilege persistedPriv = getMSentryPrivilege(convertToTSentryPrivilege(currentPrivilege), pm);
    if (persistedPriv == null) {
      persistedPriv = convertToMSentryPrivilege(convertToTSentryPrivilege(currentPrivilege));
    }

    if (requestedPrivToRevoke.getAction().equalsIgnoreCase("ALL") || requestedPrivToRevoke.getAction().equalsIgnoreCase("*")) {
      persistedPriv.removeRole(mRole);
      privCleaner.incPrivRemoval();
      pm.makePersistent(persistedPriv);
    } else if (requestedPrivToRevoke.getAction().equalsIgnoreCase(AccessConstants.SELECT)
        && !currentPrivilege.getAction().equalsIgnoreCase(AccessConstants.INSERT)) {
      revokeRolePartial(pm, mRole, currentPrivilege, persistedPriv, AccessConstants.INSERT);
    } else if (requestedPrivToRevoke.getAction().equalsIgnoreCase(AccessConstants.INSERT)
        && !currentPrivilege.getAction().equalsIgnoreCase(AccessConstants.SELECT)) {
      revokeRolePartial(pm, mRole, currentPrivilege, persistedPriv, AccessConstants.SELECT);
    }
  }

  private void revokeRolePartial(PersistenceManager pm, MSentryRole mRole,
      MSentryPrivilege currentPrivilege, MSentryPrivilege persistedPriv, String addAction)
      throws SentryInvalidInputException {
    // If table / URI, remove ALL
    persistedPriv.removeRole(mRole);
    privCleaner.incPrivRemoval();
    pm.makePersistent(persistedPriv);

    currentPrivilege.setAction(AccessConstants.ALL);
    persistedPriv = getMSentryPrivilege(convertToTSentryPrivilege(currentPrivilege), pm);
    if (persistedPriv != null && mRole.getPrivileges().contains(persistedPriv)) {
      persistedPriv.removeRole(mRole);
      privCleaner.incPrivRemoval();
      pm.makePersistent(persistedPriv);

      currentPrivilege.setAction(addAction);
      persistedPriv = getMSentryPrivilege(convertToTSentryPrivilege(currentPrivilege), pm);
      if (persistedPriv == null) {
        persistedPriv = convertToMSentryPrivilege(convertToTSentryPrivilege(currentPrivilege));
        mRole.appendPrivilege(persistedPriv);
      }
      persistedPriv.appendRole(mRole);
      pm.makePersistent(persistedPriv);
    }
  }

  /**
   * Revoke privilege from role
   */
  private void revokePrivilegeFromRole(PersistenceManager pm, TSentryPrivilege tPrivilege,
      MSentryRole mRole, MSentryPrivilege mPrivilege) throws SentryInvalidInputException {
    if (PARTIAL_REVOKE_ACTIONS.contains(mPrivilege.getAction())) {
      // if this privilege is in {ALL,SELECT,INSERT}
      // we will do partial revoke
      revokePartial(pm, tPrivilege, mRole, mPrivilege);
    } else {
      // if this privilege is not ALL, SELECT nor INSERT,
      // we will revoke it from role directly
      MSentryPrivilege persistedPriv = getMSentryPrivilege(convertToTSentryPrivilege(mPrivilege), pm);
      if (persistedPriv != null) {
        mPrivilege.removeRole(mRole);
        privCleaner.incPrivRemoval();
        pm.makePersistent(mPrivilege);
      }
    }
  }

  /**
   * Explore Privilege graph and collect child privileges.
   * The responsibility to commit/rollback the transaction should be handled by the caller.
   */
  private void populateChildren(PersistenceManager pm, Set<String> roleNames, MSentryPrivilege priv,
      Set<MSentryPrivilege> children) throws SentryInvalidInputException {
    Preconditions.checkNotNull(pm);
    if (!isNULL(priv.getServerName()) || !isNULL(priv.getDbName())
        || !isNULL(priv.getTableName())) {
      // Get all TableLevel Privs
      Set<MSentryPrivilege> childPrivs = getChildPrivileges(pm, roleNames, priv);
      for (MSentryPrivilege childPriv : childPrivs) {
        // Only recurse for table level privs..
        if (!isNULL(childPriv.getDbName()) && !isNULL(childPriv.getTableName())
            && !isNULL(childPriv.getColumnName())) {
          populateChildren(pm, roleNames, childPriv, children);
        }
        // The method getChildPrivileges() didn't do filter on "action",
        // if the action is not "All", it should judge the action of children privilege.
        // For example: a user has a privilege “All on Col1”,
        // if the operation is “REVOKE INSERT on table”
        // the privilege should be the child of table level privilege.
        // but the privilege may still have other meaning, likes "SELECT on Col1".
        // and the privileges like "SELECT on Col1" should not be revoke.
        if (!priv.isActionALL()) {
          if (childPriv.isActionALL()) {
            // If the child privilege is All, we should convert it to the same
            // privilege with parent
            childPriv.setAction(priv.getAction());
          }
          // Only include privilege that imply the parent privilege.
          if (!priv.implies(childPriv)) {
            continue;
          }
        }
        children.add(childPriv);
      }
    }
  }

  private Set<MSentryPrivilege> getChildPrivileges(PersistenceManager pm, Set<String> roleNames,
      MSentryPrivilege parent) throws SentryInvalidInputException {
    // Column and URI do not have children
    if (!isNULL(parent.getColumnName()) || !isNULL(parent.getURI())) {
      return new HashSet<MSentryPrivilege>();
    }

    Query query = pm.newQuery(MSentryPrivilege.class);
    query.declareVariables("MSentryRole role");
    List<String> rolesFiler = new LinkedList<String>();
    for (String rName : roleNames) {
      rolesFiler.add("role.roleName == \"" + trimAndLower(rName) + "\"");
    }
    StringBuilder filters = new StringBuilder("roles.contains(role) "
        + "&& (" + Joiner.on(" || ").join(rolesFiler) + ")");
    filters.append(" && serverName == \"" + parent.getServerName() + "\"");
    if (!isNULL(parent.getDbName())) {
      filters.append(" && dbName == \"" + parent.getDbName() + "\"");
      if (!isNULL(parent.getTableName())) {
        filters.append(" && tableName == \"" + parent.getTableName() + "\"");
        filters.append(" && columnName != \"__NULL__\"");
      } else {
        filters.append(" && tableName != \"__NULL__\"");
      }
    } else {
      filters.append(" && (dbName != \"__NULL__\" || URI != \"__NULL__\")");
    }

    query.setFilter(filters.toString());
    query.setResult("privilegeScope, serverName, dbName, tableName, columnName," +
        " URI, action, grantOption");
    Set<MSentryPrivilege> privileges = new HashSet<MSentryPrivilege>();
    for (Object[] privObj : (List<Object[]>) query.execute()) {
      MSentryPrivilege priv = new MSentryPrivilege();
      priv.setPrivilegeScope((String) privObj[0]);
      priv.setServerName((String) privObj[1]);
      priv.setDbName((String) privObj[2]);
      priv.setTableName((String) privObj[3]);
      priv.setColumnName((String) privObj[4]);
      priv.setURI((String) privObj[5]);
      priv.setAction((String) privObj[6]);
      priv.setGrantOption((Boolean) privObj[7]);
      privileges.add(priv);
    }
    return privileges;
  }

  private List<MSentryPrivilege> getMSentryPrivileges(TSentryPrivilege tPriv, PersistenceManager pm) {
    Query query = pm.newQuery(MSentryPrivilege.class);
    StringBuilder filters = new StringBuilder("this.serverName == \""
          + toNULLCol(safeTrimLower(tPriv.getServerName())) + "\" ");
    if (!isNULL(tPriv.getDbName())) {
      filters.append("&& this.dbName == \"" + toNULLCol(safeTrimLower(tPriv.getDbName())) + "\" ");
      if (!isNULL(tPriv.getTableName())) {
        filters.append("&& this.tableName == \"" + toNULLCol(safeTrimLower(tPriv.getTableName())) + "\" ");
        if (!isNULL(tPriv.getColumnName())) {
          filters.append("&& this.columnName == \"" + toNULLCol(safeTrimLower(tPriv.getColumnName())) + "\" ");
        }
      }
    }
    // if db is null, uri is not null
    else if (!isNULL(tPriv.getURI())){
      filters.append("&& this.URI == \"" + toNULLCol(safeTrim(tPriv.getURI())) + "\" ");
    }
    filters.append("&& this.action == \"" + toNULLCol(safeTrimLower(tPriv.getAction())) + "\"");

    query.setFilter(filters.toString());
    return (List<MSentryPrivilege>) query.execute();
  }

  private MSentryPrivilege getMSentryPrivilege(TSentryPrivilege tPriv, PersistenceManager pm) {
    Query query = pm.newQuery(MSentryPrivilege.class);
    query.setFilter("this.serverName == \"" + toNULLCol(safeTrimLower(tPriv.getServerName())) + "\" "
        + "&& this.dbName == \"" + toNULLCol(safeTrimLower(tPriv.getDbName())) + "\" "
        + "&& this.tableName == \"" + toNULLCol(safeTrimLower(tPriv.getTableName())) + "\" "
        + "&& this.columnName == \"" + toNULLCol(safeTrimLower(tPriv.getColumnName())) + "\" "
        + "&& this.URI == \"" + toNULLCol(safeTrim(tPriv.getURI())) + "\" "
        + "&& this.grantOption == grantOption "
        + "&& this.action == \"" + toNULLCol(safeTrimLower(tPriv.getAction())) + "\"");
    query.declareParameters("Boolean grantOption");
    query.setUnique(true);
    Boolean grantOption = null;
    if (tPriv.getGrantOption().equals(TSentryGrantOption.TRUE)) {
      grantOption = true;
    } else if (tPriv.getGrantOption().equals(TSentryGrantOption.FALSE)) {
      grantOption = false;
    }
    Object obj = query.execute(grantOption);
    if (obj != null) {
      return (MSentryPrivilege) obj;
    }
    return null;
  }

  public CommitContext dropSentryRole(final String roleName) throws Exception {
    return (CommitContext)tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            dropSentryRoleCore(pm, roleName);
            return new CommitContext(SERVER_UUID, incrementGetSequenceId());
          }
        });
  }

  private void dropSentryRoleCore(PersistenceManager pm, String roleName)
      throws SentryNoSuchObjectException {
    String lRoleName = trimAndLower(roleName);
    Query query = pm.newQuery(MSentryRole.class);
    query.setFilter("this.roleName == t");
    query.declareParameters("java.lang.String t");
    query.setUnique(true);
    MSentryRole sentryRole = (MSentryRole) query.execute(lRoleName);
    if (sentryRole == null) {
      throw new SentryNoSuchObjectException("Role: " + lRoleName + " doesn't exist");
    } else {
      pm.retrieve(sentryRole);
      int numPrivs = sentryRole.getPrivileges().size();
      sentryRole.removePrivileges();
      // with SENTRY-398 generic model
      sentryRole.removeGMPrivileges();
      privCleaner.incPrivRemoval(numPrivs);
      pm.deletePersistent(sentryRole);
    }
  }

  public CommitContext alterSentryRoleAddGroups(final String grantorPrincipal,
      final String roleName, final Set<TSentryGroup> groupNames) throws Exception {
    return (CommitContext)tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            alterSentryRoleAddGroupsCore(pm, roleName, groupNames);
            return new CommitContext(SERVER_UUID, incrementGetSequenceId());
          }
        });
  }

  private void alterSentryRoleAddGroupsCore(PersistenceManager pm, String roleName,
      Set<TSentryGroup> groupNames) throws SentryNoSuchObjectException {
    String lRoleName = trimAndLower(roleName);
    Query query = pm.newQuery(MSentryRole.class);
    query.setFilter("this.roleName == t");
    query.declareParameters("java.lang.String t");
    query.setUnique(true);
    MSentryRole role = (MSentryRole) query.execute(lRoleName);
    if (role == null) {
      throw new SentryNoSuchObjectException("Role: " + lRoleName + " doesn't exist");
    } else {
      query = pm.newQuery(MSentryGroup.class);
      query.setFilter("this.groupName == t");
      query.declareParameters("java.lang.String t");
      query.setUnique(true);
      List<MSentryGroup> groups = Lists.newArrayList();
      for (TSentryGroup tGroup : groupNames) {
        String groupName = tGroup.getGroupName().trim();
        MSentryGroup group = (MSentryGroup) query.execute(groupName);
        if (group == null) {
          group = new MSentryGroup(groupName, System.currentTimeMillis(), Sets.newHashSet(role));
        }
        group.appendRole(role);
        groups.add(group);
      }
      pm.makePersistentAll(groups);
    }
  }

  public CommitContext alterSentryRoleAddUsers(final String roleName,
      final Set<String> userNames) throws Exception {
    return (CommitContext)tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            alterSentryRoleAddUsersCore(pm, roleName, userNames);
            return new CommitContext(SERVER_UUID, incrementGetSequenceId());
          }
        });
  }

  private void alterSentryRoleAddUsersCore(PersistenceManager pm, String roleName,
      Set<String> userNames) throws SentryNoSuchObjectException {
    String trimmedRoleName = trimAndLower(roleName);
    MSentryRole role = getMSentryRole(pm, trimmedRoleName);
    if (role == null) {
      throw new SentryNoSuchObjectException("Role: " + trimmedRoleName);
    } else {
      Query query = pm.newQuery(MSentryUser.class);
      query.setFilter("this.userName == t");
      query.declareParameters("java.lang.String t");
      query.setUnique(true);
      List<MSentryUser> users = Lists.newArrayList();
      for (String userName : userNames) {
        userName = userName.trim();
        MSentryUser user = (MSentryUser) query.execute(userName);
        if (user == null) {
          user = new MSentryUser(userName, System.currentTimeMillis(), Sets.newHashSet(role));
        }
        user.appendRole(role);
        users.add(user);
      }
      pm.makePersistentAll(users);
    }
  }

  public CommitContext alterSentryRoleDeleteUsers(final String roleName,
      final Set<String> userNames) throws Exception {
    return (CommitContext)tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            String trimmedRoleName = trimAndLower(roleName);
            MSentryRole role = getMSentryRole(pm, trimmedRoleName);
            if (role == null) {
              throw new SentryNoSuchObjectException("Role: " + trimmedRoleName);
            } else {
              Query query = pm.newQuery(MSentryUser.class);
              query.setFilter("this.userName == t");
              query.declareParameters("java.lang.String t");
              query.setUnique(true);
              List<MSentryUser> users = Lists.newArrayList();
              for (String userName : userNames) {
                userName = userName.trim();
                MSentryUser user = (MSentryUser) query.execute(userName);
                if (user != null) {
                  user.removeRole(role);
                  users.add(user);
                }
              }
              pm.makePersistentAll(users);
              return new CommitContext(SERVER_UUID, incrementGetSequenceId());
            }
          }
        });

  }

  public CommitContext alterSentryRoleDeleteGroups(final String roleName,
      final Set<TSentryGroup> groupNames) throws Exception {
    return (CommitContext)tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            String trimmedRoleName = trimAndLower(roleName);
            Query query = pm.newQuery(MSentryRole.class);
            query.setFilter("this.roleName == t");
            query.declareParameters("java.lang.String t");
            query.setUnique(true);
            MSentryRole role = (MSentryRole) query.execute(trimmedRoleName);
            if (role == null) {
              throw new SentryNoSuchObjectException("Role: " + trimmedRoleName + " doesn't exist");
            } else {
              query = pm.newQuery(MSentryGroup.class);
              query.setFilter("this.groupName == t");
              query.declareParameters("java.lang.String t");
              query.setUnique(true);
              List<MSentryGroup> groups = Lists.newArrayList();
              for (TSentryGroup tGroup : groupNames) {
                String groupName = tGroup.getGroupName().trim();
                MSentryGroup group = (MSentryGroup) query.execute(groupName);
                if (group != null) {
                  group.removeRole(role);
                  groups.add(group);
                }
              }
              pm.makePersistentAll(groups);
              return new CommitContext(SERVER_UUID, incrementGetSequenceId());
            }
          }
        });
  }

  @VisibleForTesting
  MSentryRole getMSentryRoleByName(final String roleName) throws Exception {
    return (MSentryRole)tm.executeTransaction(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            String trimmedRoleName = trimAndLower(roleName);
            Query query = pm.newQuery(MSentryRole.class);
            query.setFilter("this.roleName == t");
            query.declareParameters("java.lang.String t");
            query.setUnique(true);
            MSentryRole sentryRole = (MSentryRole) query.execute(trimmedRoleName);
            if (sentryRole == null) {
              throw new SentryNoSuchObjectException("Role: " + trimmedRoleName + " doesn't exist");
            } else {
              pm.retrieve(sentryRole);
            }
            return sentryRole;
          }
        });
  }

  private boolean hasAnyServerPrivileges(final Set<String> roleNames, final String serverName) {
    if (roleNames == null || roleNames.isEmpty()) {
      return false;
    }
    boolean result = false;
    try {
      result = (Boolean) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              Query query = pm.newQuery(MSentryPrivilege.class);
              query.declareVariables("org.apache.sentry.provider.db.service.model.MSentryRole role");
              List<String> rolesFiler = new LinkedList<String>();
              for (String rName : roleNames) {
                rolesFiler.add("role.roleName == \"" + trimAndLower(rName) + "\"");
              }
              StringBuilder filters = new StringBuilder("roles.contains(role) "
                  + "&& (" + Joiner.on(" || ").join(rolesFiler) + ") ");
              filters.append("&& serverName == \"" + trimAndLower(serverName) + "\"");
              query.setFilter(filters.toString());
              query.setResult("count(this)");
              Long numPrivs = (Long) query.execute();
              return numPrivs > 0;
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  List<MSentryPrivilege> getMSentryPrivileges(final Set<String> roleNames,
      final TSentryAuthorizable authHierarchy) {
    List<MSentryPrivilege> result = new ArrayList<MSentryPrivilege>();
    if (roleNames == null || roleNames.isEmpty()) {
      return result;
    }

    try {
      result = (List<MSentryPrivilege>) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              Query query = pm.newQuery(MSentryPrivilege.class);
              query.declareVariables("MSentryRole role");
              List<String> rolesFiler = new LinkedList<String>();
              for (String rName : roleNames) {
                rolesFiler.add("role.roleName == \"" + trimAndLower(rName) + "\"");
              }
              StringBuilder filters = new StringBuilder("roles.contains(role) "
                  + "&& (" + Joiner.on(" || ").join(rolesFiler) + ") ");
              if (authHierarchy != null && authHierarchy.getServer() != null) {
                filters.append("&& serverName == \"" + authHierarchy.getServer().toLowerCase() + "\"");
                if (authHierarchy.getDb() != null) {
                  filters.append(" && ((dbName == \"" + authHierarchy.getDb().toLowerCase() + "\") || (dbName == \"__NULL__\")) && (URI == \"__NULL__\")");
                  if (authHierarchy.getTable() != null
                      && !AccessConstants.ALL.equalsIgnoreCase(authHierarchy.getTable())) {
                    if (!AccessConstants.SOME.equalsIgnoreCase(authHierarchy.getTable())) {
                      filters.append(" && ((tableName == \"" + authHierarchy.getTable().toLowerCase() + "\") || (tableName == \"__NULL__\")) && (URI == \"__NULL__\")");
                    }
                    if (authHierarchy.getColumn() != null
                        && !AccessConstants.ALL.equalsIgnoreCase(authHierarchy.getColumn())
                        && !AccessConstants.SOME.equalsIgnoreCase(authHierarchy.getColumn())) {
                      filters.append(" && ((columnName == \"" + authHierarchy.getColumn().toLowerCase() + "\") || (columnName == \"__NULL__\")) && (URI == \"__NULL__\")");
                    }
                  }
                }
                if (authHierarchy.getUri() != null) {
                  filters.append(" && ((URI != \"__NULL__\") && (\"" + authHierarchy.getUri() + "\".startsWith(URI)) || (URI == \"__NULL__\")) && (dbName == \"__NULL__\")");
                }
              }
              query.setFilter(filters.toString());
              return  (List<MSentryPrivilege>) query.execute();
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  List<MSentryPrivilege> getMSentryPrivilegesByAuth(final Set<String> roleNames,
      final TSentryAuthorizable authHierarchy) {
    List<MSentryPrivilege> result = new ArrayList<MSentryPrivilege>();
    try {
      result = (List<MSentryPrivilege>) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              Query query = pm.newQuery(MSentryPrivilege.class);
              StringBuilder filters = new StringBuilder();
              if (roleNames == null || roleNames.isEmpty()) {
                filters.append(" !roles.isEmpty() ");
              } else {
                query.declareVariables("MSentryRole role");
                List<String> rolesFiler = new LinkedList<String>();
                for (String rName : roleNames) {
                  rolesFiler.add("role.roleName == \"" + trimAndLower(rName) + "\"");
                }
                filters.append("roles.contains(role) "
                    + "&& (" + Joiner.on(" || ").join(rolesFiler) + ") ");
              }
              if (authHierarchy.getServer() != null) {
                filters.append("&& serverName == \"" +
                    authHierarchy.getServer().toLowerCase() + "\"");
                if (authHierarchy.getDb() != null) {
                  filters.append(" && (dbName == \"" +
                      authHierarchy.getDb().toLowerCase() + "\") && (URI == \"__NULL__\")");
                  if (authHierarchy.getTable() != null) {
                    filters.append(" && (tableName == \"" +
                        authHierarchy.getTable().toLowerCase() + "\")");
                  } else {
                    filters.append(" && (tableName == \"__NULL__\")");
                  }
                } else if (authHierarchy.getUri() != null) {
                  filters.append(" && (URI != \"__NULL__\") && (\"" + authHierarchy.getUri() +
                      "\".startsWith(URI)) && (dbName == \"__NULL__\")");
                } else {
                  filters.append(" && (dbName == \"__NULL__\") && (URI == \"__NULL__\")");
                }
              } else {
                // if no server, then return empty resultset
                return new ArrayList<MSentryPrivilege>();
              }
              FetchGroup grp = pm.getFetchGroup(MSentryPrivilege.class, "fetchRole");
              grp.addMember("roles");
              pm.getFetchPlan().addGroup("fetchRole");
              query.setFilter(filters.toString());
              return (List<MSentryPrivilege>) query.execute();
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  public TSentryPrivilegeMap listSentryPrivilegesByAuthorizable(Set<String> groups,
      TSentryActiveRoleSet activeRoles,
      TSentryAuthorizable authHierarchy, boolean isAdmin)
      throws SentryInvalidInputException {
    Map<String, Set<TSentryPrivilege>> resultPrivilegeMap = Maps.newTreeMap();
    Set<String> roles = getRolesToQuery(groups, null, new TSentryActiveRoleSet(true, null));

    if (activeRoles != null && !activeRoles.isAll()) {
      // need to check/convert to lowercase here since this is from user input
      for (String aRole : activeRoles.getRoles()) {
        roles.add(aRole.toLowerCase());
      }
    }

    // An empty 'roles' is a treated as a wildcard (in case of admin role)..
    // so if not admin, don't return anything if 'roles' is empty..
    if (isAdmin || !roles.isEmpty()) {
      List<MSentryPrivilege> mSentryPrivileges = getMSentryPrivilegesByAuth(roles,
          authHierarchy);
      for (MSentryPrivilege priv : mSentryPrivileges) {
        for (MSentryRole role : priv.getRoles()) {
          TSentryPrivilege tPriv = convertToTSentryPrivilege(priv);
          if (resultPrivilegeMap.containsKey(role.getRoleName())) {
            resultPrivilegeMap.get(role.getRoleName()).add(tPriv);
          } else {
            Set<TSentryPrivilege> tPrivSet = Sets.newTreeSet();
            tPrivSet.add(tPriv);
            resultPrivilegeMap.put(role.getRoleName(), tPrivSet);
          }
        }
      }
    }
    return new TSentryPrivilegeMap(resultPrivilegeMap);
  }

  private Set<MSentryPrivilege> getMSentryPrivilegesByRoleName(String roleName)
      throws Exception {
    MSentryRole mSentryRole = getMSentryRoleByName(roleName);
    return mSentryRole.getPrivileges();
  }

  /**
   * Gets sentry privilege objects for a given roleName from the persistence layer
   * @param roleName : roleName to look up
   * @return : Set of thrift sentry privilege objects
   * @throws Exception
   */

  public Set<TSentryPrivilege> getAllTSentryPrivilegesByRoleName(String roleName)
      throws Exception {
    return convertToTSentryPrivileges(getMSentryPrivilegesByRoleName(roleName));
  }


  /**
   * Gets sentry privilege objects for criteria from the persistence layer
   * @param roleNames : roleNames to look up (required)
   * @param authHierarchy : filter push down based on auth hierarchy (optional)
   * @return : Set of thrift sentry privilege objects
   * @throws SentryNoSuchObjectException
   */

  public Set<TSentryPrivilege> getTSentryPrivileges(Set<String> roleNames, TSentryAuthorizable authHierarchy) throws SentryInvalidInputException {
    if (authHierarchy.getServer() == null) {
      throw new SentryInvalidInputException("serverName cannot be null !!");
    }
    if (authHierarchy.getTable() != null && authHierarchy.getDb() == null) {
      throw new SentryInvalidInputException("dbName cannot be null when tableName is present !!");
    }
    if (authHierarchy.getColumn() != null && authHierarchy.getTable() == null) {
      throw new SentryInvalidInputException("tableName cannot be null when columnName is present !!");
    }
    if (authHierarchy.getUri() == null && authHierarchy.getDb() == null) {
      throw new SentryInvalidInputException("One of uri or dbName must not be null !!");
    }
    return convertToTSentryPrivileges(getMSentryPrivileges(roleNames, authHierarchy));
  }


  private Set<MSentryRole> getMSentryRolesByGroupName(final String groupName)
      throws Exception {
    return (Set<MSentryRole>) tm.executeTransaction(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            Set<MSentryRole> roles;

            //If no group name was specified, return all roles
            if (groupName == null) {
              Query query = pm.newQuery(MSentryRole.class);
              roles = new HashSet<MSentryRole>((List<MSentryRole>)query.execute());
            } else {
              Query query = pm.newQuery(MSentryGroup.class);
              MSentryGroup sentryGroup;
              String trimmedGroupName = groupName.trim();
              query.setFilter("this.groupName == t");
              query.declareParameters("java.lang.String t");
              query.setUnique(true);
              sentryGroup = (MSentryGroup) query.execute(trimmedGroupName);
              if (sentryGroup == null) {
                throw new SentryNoSuchObjectException("Group: " + trimmedGroupName + " doesn't exist");
              } else {
                pm.retrieve(sentryGroup);
              }
              roles = sentryGroup.getRoles();
            }
            for (MSentryRole role: roles) {
              pm.retrieve(role);
            }
            return roles;
          }
        });
  }

  /**
   * Gets sentry role objects for a given groupName from the persistence layer
   * @param groupName : groupName to look up ( if null returns all roles for all groups)
   * @return : Set of thrift sentry role objects
   * @throws SentryNoSuchObjectException
   */
  public Set<TSentryRole> getTSentryRolesByGroupName(Set<String> groupNames,
      boolean checkAllGroups) throws Exception {
    Set<MSentryRole> roleSet = Sets.newHashSet();
    for (String groupName : groupNames) {
      try {
        roleSet.addAll(getMSentryRolesByGroupName(groupName));
      } catch (SentryNoSuchObjectException e) {
        // if we are checking for all the given groups, then continue searching
        if (!checkAllGroups) {
          throw e;
        }
      }
    }
    return convertToTSentryRoles(roleSet);
  }

  public Set<String> getRoleNamesForGroups(final Set<String> groups) {
    if (groups == null || groups.isEmpty()) {
      return ImmutableSet.of();
    }

    Set<String> result = new HashSet<>();
    try {
      result = (Set<String>) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              return getRoleNamesForGroupsCore(pm, groups);
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  private Set<String> getRoleNamesForGroupsCore(PersistenceManager pm, Set<String> groups) {
    return convertToRoleNameSet(getRolesForGroups(pm, groups));
  }

  public Set<String> getRoleNamesForUsers(final Set<String> users) {
    if (users == null || users.isEmpty()) {
      return ImmutableSet.of();
    }

    Set<String> result = new HashSet<>();
    try {
      result = (Set<String>) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              return getRoleNamesForUsersCore(pm,users);
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  private Set<String> getRoleNamesForUsersCore(PersistenceManager pm, Set<String> users) {
    return convertToRoleNameSet(getRolesForUsers(pm, users));
  }

  public Set<TSentryRole> getTSentryRolesByUserNames(final Set<String> users) {
    Set<TSentryRole> result = new HashSet<>();

    try {
      result = (Set<TSentryRole>) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              Set<MSentryRole> mSentryRoles = getRolesForUsers(pm, users);
              // Since {@link MSentryRole#getGroups()} is lazy-loading, the converting should be call
              // before transaction committed.
              return convertToTSentryRoles(mSentryRoles);
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  public Set<MSentryRole> getRolesForGroups(PersistenceManager pm, Set<String> groups) {
    Set<MSentryRole> result = Sets.newHashSet();
    if (groups != null) {
      Query query = pm.newQuery(MSentryGroup.class);
      query.setFilter("this.groupName == t");
      query.declareParameters("java.lang.String t");
      query.setUnique(true);
      for (String group : groups) {
        MSentryGroup sentryGroup = (MSentryGroup) query.execute(group.trim());
        if (sentryGroup != null) {
          result.addAll(sentryGroup.getRoles());
        }
      }
    }
    return result;
  }

  public Set<MSentryRole> getRolesForUsers(PersistenceManager pm, Set<String> users) {
    Set<MSentryRole> result = Sets.newHashSet();
    if (users != null) {
      Query query = pm.newQuery(MSentryUser.class);
      query.setFilter("this.userName == t");
      query.declareParameters("java.lang.String t");
      query.setUnique(true);
      for (String user : users) {
        MSentryUser sentryUser = (MSentryUser) query.execute(user.trim());
        if (sentryUser != null) {
          result.addAll(sentryUser.getRoles());
        }
      }
    }
    return result;
  }

  public Set<String> listAllSentryPrivilegesForProvider(Set<String> groups, Set<String> users,
      TSentryActiveRoleSet roleSet) throws SentryInvalidInputException {
    return listSentryPrivilegesForProvider(groups, users, roleSet, null);
  }


  public Set<String> listSentryPrivilegesForProvider(Set<String> groups, Set<String> users,
      TSentryActiveRoleSet roleSet, TSentryAuthorizable authHierarchy) throws SentryInvalidInputException {
    Set<String> result = Sets.newHashSet();
    Set<String> rolesToQuery = getRolesToQuery(groups, users, roleSet);
    List<MSentryPrivilege> mSentryPrivileges = getMSentryPrivileges(rolesToQuery, authHierarchy);
    for (MSentryPrivilege priv : mSentryPrivileges) {
      result.add(toAuthorizable(priv));
    }

    return result;
  }

  public boolean hasAnyServerPrivileges(Set<String> groups, Set<String> users,
      TSentryActiveRoleSet roleSet, String server) {
    Set<String> rolesToQuery = getRolesToQuery(groups, users, roleSet);
    return hasAnyServerPrivileges(rolesToQuery, server);
  }

  private Set<String> getRolesToQuery(final Set<String> groups, final Set<String> users,
      final TSentryActiveRoleSet roleSet) {
    Set<String> result = new HashSet<>();
    try {
      result = (Set<String>) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              Set<String> activeRoleNames = toTrimedLower(roleSet.getRoles());

              Set<String> roleNames = Sets.newHashSet();
              roleNames.addAll(toTrimedLower(getRoleNamesForGroupsCore(pm, groups)));
              roleNames.addAll(toTrimedLower(getRoleNamesForUsersCore(pm, users)));
              return roleSet.isAll() ? roleNames : Sets.intersection(activeRoleNames,
                  roleNames);
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  @VisibleForTesting
  static String toAuthorizable(MSentryPrivilege privilege) {
    List<String> authorizable = new ArrayList<String>(4);
    authorizable.add(KV_JOINER.join(AuthorizableType.Server.name().toLowerCase(),
        privilege.getServerName()));
    if (isNULL(privilege.getURI())) {
      if (!isNULL(privilege.getDbName())) {
        authorizable.add(KV_JOINER.join(AuthorizableType.Db.name().toLowerCase(),
            privilege.getDbName()));
        if (!isNULL(privilege.getTableName())) {
          authorizable.add(KV_JOINER.join(AuthorizableType.Table.name().toLowerCase(),
              privilege.getTableName()));
          if (!isNULL(privilege.getColumnName())) {
            authorizable.add(KV_JOINER.join(AuthorizableType.Column.name().toLowerCase(),
                privilege.getColumnName()));
          }
        }
      }
    } else {
      authorizable.add(KV_JOINER.join(AuthorizableType.URI.name().toLowerCase(),
          privilege.getURI()));
    }
    if (!isNULL(privilege.getAction())
        && !privilege.getAction().equalsIgnoreCase(AccessConstants.ALL)) {
      authorizable
      .add(KV_JOINER.join(SentryConstants.PRIVILEGE_NAME.toLowerCase(),
          privilege.getAction()));
    }
    return AUTHORIZABLE_JOINER.join(authorizable);
  }

  @VisibleForTesting
  static Set<String> toTrimedLower(Set<String> s) {
    if (null == s) {
      return new HashSet<String>();
    }
    Set<String> result = Sets.newHashSet();
    for (String v : s) {
      result.add(v.trim().toLowerCase());
    }
    return result;
  }


  /**
   * Converts model object(s) to thrift object(s).
   * Additionally does normalization
   * such as trimming whitespace and setting appropriate case. Also sets the create
   * time.
   */

  private Set<TSentryPrivilege> convertToTSentryPrivileges(Collection<MSentryPrivilege> mSentryPrivileges) {
    Set<TSentryPrivilege> privileges = new HashSet<TSentryPrivilege>();
    for(MSentryPrivilege mSentryPrivilege:mSentryPrivileges) {
      privileges.add(convertToTSentryPrivilege(mSentryPrivilege));
    }
    return privileges;
  }

  private Set<TSentryRole> convertToTSentryRoles(Set<MSentryRole> mSentryRoles) {
    Set<TSentryRole> roles = new HashSet<TSentryRole>();
    for(MSentryRole mSentryRole:mSentryRoles) {
      roles.add(convertToTSentryRole(mSentryRole));
    }
    return roles;
  }

  private Set<String> convertToRoleNameSet(Set<MSentryRole> mSentryRoles) {
    Set<String> roleNameSet = Sets.newHashSet();
    for (MSentryRole role : mSentryRoles) {
      roleNameSet.add(role.getRoleName());
    }
    return roleNameSet;
  }

  private TSentryRole convertToTSentryRole(MSentryRole mSentryRole) {
    TSentryRole role = new TSentryRole();
    role.setRoleName(mSentryRole.getRoleName());
    role.setGrantorPrincipal("--");
    Set<TSentryGroup> sentryGroups = new HashSet<TSentryGroup>();
    for(MSentryGroup mSentryGroup:mSentryRole.getGroups()) {
      TSentryGroup group = convertToTSentryGroup(mSentryGroup);
      sentryGroups.add(group);
    }

    role.setGroups(sentryGroups);
    return role;
  }

  private TSentryGroup convertToTSentryGroup(MSentryGroup mSentryGroup) {
    TSentryGroup group = new TSentryGroup();
    group.setGroupName(mSentryGroup.getGroupName());
    return group;
  }

  protected TSentryPrivilege convertToTSentryPrivilege(MSentryPrivilege mSentryPrivilege) {
    TSentryPrivilege privilege = new TSentryPrivilege();
    convertToTSentryPrivilege(mSentryPrivilege, privilege);
    return privilege;
  }

  private void convertToTSentryPrivilege(MSentryPrivilege mSentryPrivilege,
      TSentryPrivilege privilege) {
    privilege.setCreateTime(mSentryPrivilege.getCreateTime());
    privilege.setAction(fromNULLCol(mSentryPrivilege.getAction()));
    privilege.setPrivilegeScope(mSentryPrivilege.getPrivilegeScope());
    privilege.setServerName(fromNULLCol(mSentryPrivilege.getServerName()));
    privilege.setDbName(fromNULLCol(mSentryPrivilege.getDbName()));
    privilege.setTableName(fromNULLCol(mSentryPrivilege.getTableName()));
    privilege.setColumnName(fromNULLCol(mSentryPrivilege.getColumnName()));
    privilege.setURI(fromNULLCol(mSentryPrivilege.getURI()));
    if (mSentryPrivilege.getGrantOption() != null) {
      privilege.setGrantOption(TSentryGrantOption.valueOf(mSentryPrivilege.getGrantOption().toString().toUpperCase()));
    } else {
      privilege.setGrantOption(TSentryGrantOption.UNSET);
    }
  }

  /**
   * Converts thrift object to model object. Additionally does normalization
   * such as trimming whitespace and setting appropriate case.
   * @throws SentryInvalidInputException
   */
  private MSentryPrivilege convertToMSentryPrivilege(TSentryPrivilege privilege)
      throws SentryInvalidInputException {
    MSentryPrivilege mSentryPrivilege = new MSentryPrivilege();
    mSentryPrivilege.setServerName(toNULLCol(safeTrimLower(privilege.getServerName())));
    mSentryPrivilege.setDbName(toNULLCol(safeTrimLower(privilege.getDbName())));
    mSentryPrivilege.setTableName(toNULLCol(safeTrimLower(privilege.getTableName())));
    mSentryPrivilege.setColumnName(toNULLCol(safeTrimLower(privilege.getColumnName())));
    mSentryPrivilege.setPrivilegeScope(safeTrim(privilege.getPrivilegeScope()));
    mSentryPrivilege.setAction(toNULLCol(safeTrimLower(privilege.getAction())));
    mSentryPrivilege.setCreateTime(System.currentTimeMillis());
    mSentryPrivilege.setURI(toNULLCol(safeTrim(privilege.getURI())));
    if ( !privilege.getGrantOption().equals(TSentryGrantOption.UNSET) ) {
      mSentryPrivilege.setGrantOption(Boolean.valueOf(privilege.getGrantOption().toString()));
    } else {
      mSentryPrivilege.setGrantOption(null);
    }
    return mSentryPrivilege;
  }
  private static String safeTrim(String s) {
    if (s == null) {
      return null;
    }
    return s.trim();
  }
  private static String safeTrimLower(String s) {
    if (s == null) {
      return null;
    }
    return s.trim().toLowerCase();
  }

  public String getSentryVersion() throws Exception {
    MSentryVersion mVersion = getMSentryVersion();
    return mVersion.getSchemaVersion();
  }

  public void setSentryVersion(final String newVersion, final String verComment)
      throws Exception {
    tm.executeTransaction(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            MSentryVersion mVersion;
            try {
              mVersion = getMSentryVersion();
              if (newVersion.equals(mVersion.getSchemaVersion())) {
                // specified version already in there
                return null;
              }
            } catch (SentryNoSuchObjectException e) {
              // if the version doesn't exist, then create it
              mVersion = new MSentryVersion();
            }
            mVersion.setSchemaVersion(newVersion);
            mVersion.setVersionComment(verComment);
            pm.makePersistent(mVersion);
            return null;
          }
        });
  }

  @SuppressWarnings("unchecked")
  private MSentryVersion getMSentryVersion() throws Exception {
    return (MSentryVersion) tm.executeTransaction(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            try {
              Query query = pm.newQuery(MSentryVersion.class);
              List<MSentryVersion> mSentryVersions = (List<MSentryVersion>) query
                  .execute();
              pm.retrieveAll(mSentryVersions);
              if (mSentryVersions.isEmpty()) {
                throw new SentryNoSuchObjectException("No matching version found");
              }
              if (mSentryVersions.size() > 1) {
                throw new SentryAccessDeniedException(
                    "Metastore contains multiple versions");
              }
              return mSentryVersions.get(0);
            } catch (JDODataStoreException e) {
              if (e.getCause() instanceof MissingTableException) {
                throw new SentryAccessDeniedException("Version table not found. "
                    + "The sentry store is not set or corrupt ");
              } else {
                throw e;
              }
            }
          }
        });
  }

  /**
   * Drop given privilege from all roles
   */
  public void dropPrivilege(final TSentryAuthorizable tAuthorizable) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {

            TSentryPrivilege tPrivilege = toSentryPrivilege(tAuthorizable);
            try {
              if (isMultiActionsSupported(tPrivilege)) {
                for (String privilegeAction : ALL_ACTIONS) {
                  tPrivilege.setAction(privilegeAction);
                  dropPrivilegeForAllRoles(pm, new TSentryPrivilege(tPrivilege));
                }
              } else {
                dropPrivilegeForAllRoles(pm, new TSentryPrivilege(tPrivilege));
              }
            } catch (JDODataStoreException e) {
              throw new SentryInvalidInputException("Failed to get privileges: "
                  + e.getMessage());
            }
            return null;
          }
        });
  }

  /**
   * Rename given privilege from all roles drop the old privilege and create the new one
   * @param tAuthorizable
   * @param newTAuthorizable
   * @throws SentryNoSuchObjectException
   * @throws SentryInvalidInputException
   */
  public void renamePrivilege(final TSentryAuthorizable tAuthorizable,
      final TSentryAuthorizable newTAuthorizable) throws Exception {
    tm.executeTransactionWithRetry(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {

            TSentryPrivilege tPrivilege = toSentryPrivilege(tAuthorizable);
            TSentryPrivilege newPrivilege = toSentryPrivilege(newTAuthorizable);
            try {
              // In case of tables or DBs, check all actions
              if (isMultiActionsSupported(tPrivilege)) {
                for (String privilegeAction : ALL_ACTIONS) {
                  tPrivilege.setAction(privilegeAction);
                  newPrivilege.setAction(privilegeAction);
                  renamePrivilegeForAllRoles(pm, tPrivilege, newPrivilege);
                }
              } else {
                renamePrivilegeForAllRoles(pm, tPrivilege, newPrivilege);
              }
            } catch (JDODataStoreException e) {
              throw new SentryInvalidInputException("Failed to get privileges: "
                  + e.getMessage());
            }
            return null;
          }
        });
  }

  // Currently INSERT/SELECT/ALL are supported for Table and DB level privileges
  private boolean isMultiActionsSupported(TSentryPrivilege tPrivilege) {
    return tPrivilege.getDbName() != null;

  }
  // wrapper for dropOrRename
  private void renamePrivilegeForAllRoles(PersistenceManager pm,
      TSentryPrivilege tPrivilege,
      TSentryPrivilege newPrivilege) throws SentryNoSuchObjectException,
      SentryInvalidInputException {
    dropOrRenamePrivilegeForAllRoles(pm, tPrivilege, newPrivilege);
  }

  /**
   * Drop given privilege from all roles
   * @param tPrivilege
   * @throws SentryNoSuchObjectException
   * @throws SentryInvalidInputException
   */
  private void dropPrivilegeForAllRoles(PersistenceManager pm,
      TSentryPrivilege tPrivilege)
      throws SentryNoSuchObjectException, SentryInvalidInputException {
    dropOrRenamePrivilegeForAllRoles(pm, tPrivilege, null);
  }

  /**
   * Drop given privilege from all roles Create the new privilege if asked
   * @param tPrivilege
   * @param pm
   * @throws SentryNoSuchObjectException
   * @throws SentryInvalidInputException
   */
  private void dropOrRenamePrivilegeForAllRoles(PersistenceManager pm,
      TSentryPrivilege tPrivilege,
      TSentryPrivilege newTPrivilege) throws SentryNoSuchObjectException,
      SentryInvalidInputException {
    HashSet<MSentryRole> roleSet = Sets.newHashSet();

    List<MSentryPrivilege> mPrivileges = getMSentryPrivileges(tPrivilege, pm);
    if (mPrivileges != null && !mPrivileges.isEmpty()) {
      for (MSentryPrivilege mPrivilege : mPrivileges) {
        roleSet.addAll(ImmutableSet.copyOf(mPrivilege.getRoles()));
      }
    }

    MSentryPrivilege parent = getMSentryPrivilege(tPrivilege, pm);
    for (MSentryRole role : roleSet) {
      // 1. get privilege and child privileges
      Set<MSentryPrivilege> privilegeGraph = Sets.newHashSet();
      if (parent != null) {
        privilegeGraph.add(parent);
        populateChildren(pm, Sets.newHashSet(role.getRoleName()), parent, privilegeGraph);
      } else {
        populateChildren(pm, Sets.newHashSet(role.getRoleName()), convertToMSentryPrivilege(tPrivilege),
            privilegeGraph);
      }
      // 2. revoke privilege and child privileges
      alterSentryRoleRevokePrivilegeCore(pm, role.getRoleName(), tPrivilege);
      // 3. add new privilege and child privileges with new tableName
      if (newTPrivilege != null) {
        for (MSentryPrivilege m : privilegeGraph) {
          TSentryPrivilege t = convertToTSentryPrivilege(m);
          if (newTPrivilege.getPrivilegeScope().equals(PrivilegeScope.DATABASE.name())) {
            t.setDbName(newTPrivilege.getDbName());
          } else if (newTPrivilege.getPrivilegeScope().equals(PrivilegeScope.TABLE.name())) {
            t.setTableName(newTPrivilege.getTableName());
          }
          alterSentryRoleGrantPrivilegeCore(pm, role.getRoleName(), t);
        }
      }
    }
  }

  private TSentryPrivilege toSentryPrivilege(TSentryAuthorizable tAuthorizable)
      throws SentryInvalidInputException {
    TSentryPrivilege tSentryPrivilege = new TSentryPrivilege();
    tSentryPrivilege.setDbName(fromNULLCol(tAuthorizable.getDb()));
    tSentryPrivilege.setServerName(fromNULLCol(tAuthorizable.getServer()));
    tSentryPrivilege.setTableName(fromNULLCol(tAuthorizable.getTable()));
    tSentryPrivilege.setColumnName(fromNULLCol(tAuthorizable.getColumn()));
    tSentryPrivilege.setURI(fromNULLCol(tAuthorizable.getUri()));
    PrivilegeScope scope;
    if (!isNULL(tSentryPrivilege.getColumnName())) {
      scope = PrivilegeScope.COLUMN;
    } else if (!isNULL(tSentryPrivilege.getTableName())) {
      scope = PrivilegeScope.TABLE;
    } else if (!isNULL(tSentryPrivilege.getDbName())) {
      scope = PrivilegeScope.DATABASE;
    } else if (!isNULL(tSentryPrivilege.getURI())) {
      scope = PrivilegeScope.URI;
    } else {
      scope = PrivilegeScope.SERVER;
    }
    tSentryPrivilege.setPrivilegeScope(scope.name());
    tSentryPrivilege.setAction(AccessConstants.ALL);
    return tSentryPrivilege;
  }

  public static String toNULLCol(String s) {
    return Strings.isNullOrEmpty(s) ? NULL_COL : s;
  }

  public static String fromNULLCol(String s) {
    return isNULL(s) ? "" : s;
  }

  public static boolean isNULL(String s) {
    return Strings.isNullOrEmpty(s) || s.equals(NULL_COL);
  }

  /**
   * Grant option check
   * @param pm
   * @param privilege
   * @throws SentryUserException
   */
  private void grantOptionCheck(PersistenceManager pm, String grantorPrincipal, TSentryPrivilege privilege)
      throws SentryUserException {
    MSentryPrivilege mPrivilege = convertToMSentryPrivilege(privilege);
    if (grantorPrincipal == null) {
      throw new SentryInvalidInputException("grantorPrincipal should not be null");
    }

    Set<String> groups = SentryPolicyStoreProcessor.getGroupsFromUserName(conf, grantorPrincipal);

    // if grantor is in adminGroup, don't need to do check
    Set<String> admins = getAdminGroups();
    boolean isAdminGroup = false;
    if (groups != null && admins != null && !admins.isEmpty()) {
      for (String g : groups) {
        if (admins.contains(g)) {
          isAdminGroup = true;
          break;
        }
      }
    }

    if (!isAdminGroup) {
      boolean hasGrant = false;
      // get all privileges for group and user
      Set<MSentryRole> roles = getRolesForGroups(pm, groups);
      roles.addAll(getRolesForUsers(pm, Sets.newHashSet(grantorPrincipal)));
      if (roles != null && !roles.isEmpty()) {
        for (MSentryRole role : roles) {
          Set<MSentryPrivilege> privilegeSet = role.getPrivileges();
          if (privilegeSet != null && !privilegeSet.isEmpty()) {
            // if role has a privilege p with grant option
            // and mPrivilege is a child privilege of p
            for (MSentryPrivilege p : privilegeSet) {
              if (p.getGrantOption() && p.implies(mPrivilege)) {
                hasGrant = true;
                break;
              }
            }
          }
        }
      }

      if (!hasGrant) {
        throw new SentryGrantDeniedException(grantorPrincipal
            + " has no grant!");
      }
    }
  }

  // get adminGroups from conf
  private Set<String> getAdminGroups() {
    return Sets.newHashSet(conf.getStrings(
        ServerConfig.ADMIN_GROUPS, new String[]{}));
  }

  /**
   * This returns a Mapping of AuthZObj(db/table) -> (Role -> permission)
   */
  public Map<String, HashMap<String, String>> retrieveFullPrivilegeImage() {
    Map<String, HashMap<String, String>> result = new HashMap<>();
    try {
      result = (Map<String, HashMap<String, String>>) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              Map<String, HashMap<String, String>> retVal = new HashMap<>();
              Query query = pm.newQuery(MSentryPrivilege.class);
              String filters = "(serverName != \"__NULL__\") "
                  + "&& (dbName != \"__NULL__\") " + "&& (URI == \"__NULL__\")";
              query.setFilter(filters.toString());
              query
                  .setOrdering("serverName ascending, dbName ascending, tableName ascending");
              List<MSentryPrivilege> privileges = (List<MSentryPrivilege>) query
                  .execute();
              for (MSentryPrivilege mPriv : privileges) {
                String authzObj = mPriv.getDbName();
                if (!isNULL(mPriv.getTableName())) {
                  authzObj = authzObj + "." + mPriv.getTableName();
                }
                HashMap<String, String> pUpdate = retVal.get(authzObj);
                if (pUpdate == null) {
                  pUpdate = new HashMap<String, String>();
                  retVal.put(authzObj, pUpdate);
                }
                for (MSentryRole mRole : mPriv.getRoles()) {
                  String existingPriv = pUpdate.get(mRole.getRoleName());
                  if (existingPriv == null) {
                    pUpdate.put(mRole.getRoleName(), mPriv.getAction().toUpperCase());
                  } else {
                    pUpdate.put(mRole.getRoleName(), existingPriv + ","
                        + mPriv.getAction().toUpperCase());
                  }
                }
              }
              return retVal;
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  /**
   * This returns a Mapping of Role -> [Groups]
   */
  public Map<String, LinkedList<String>> retrieveFullRoleImage() {
    Map<String, LinkedList<String>> result = new HashMap<>();
    try {
      result = (Map<String, LinkedList<String>>) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              Map<String, LinkedList<String>> retVal = new HashMap<>();
              Query query = pm.newQuery(MSentryGroup.class);
              List<MSentryGroup> groups = (List<MSentryGroup>) query.execute();
              for (MSentryGroup mGroup : groups) {
                for (MSentryRole role : mGroup.getRoles()) {
                  LinkedList<String> rUpdate = retVal.get(role.getRoleName());
                  if (rUpdate == null) {
                    rUpdate = new LinkedList<String>();
                    retVal.put(role.getRoleName(), rUpdate);
                  }
                  rUpdate.add(mGroup.getGroupName());
                }
              }
              return retVal;
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  /**
   * This thread exists to clean up "orphaned" privilege rows in the database.
   * These rows aren't removed automatically due to the fact that there is
   * a many-to-many mapping between the roles and privileges, and the
   * detection and removal of orphaned privileges is a wee bit involved.
   * This thread hangs out until notified by the parent (the outer class)
   * and then runs a custom SQL statement that detects and removes orphans.
   */
  private class PrivCleaner implements Runnable {
    // Kick off priv orphan removal after this many notifies
    private static final int NOTIFY_THRESHOLD = 50;

    // How many times we've been notified; reset to zero after orphan removal
    private int currentNotifies = 0;

    // Internal state for threads
    private boolean exitRequired = false;

    // This lock and condition are needed to implement a way to drop the
    // lock inside a while loop, and not hold the lock across the orphan
    // removal.
    private final Lock lock = new ReentrantLock();
    private final Condition cond = lock.newCondition();

    /**
     * Waits in a loop, running the orphan removal function when notified.
     * Will exit after exitRequired is set to true by exit().  We are careful
     * to not hold our lock while removing orphans; that operation might
     * take a long time.  There's also the matter of lock ordering.  Other
     * threads start a transaction first, and then grab our lock; this thread
     * grabs the lock and then starts a transaction.  Handling this correctly
     * requires explicit locking/unlocking through the loop.
     */
    public void run() {
      while (true) {
        lock.lock();
        try {
          // Check here in case this was set during removeOrphanedPrivileges()
          if (exitRequired) {
            return;
          }
          while (currentNotifies <= NOTIFY_THRESHOLD) {
            try {
              cond.await();
            } catch (InterruptedException e) {
              // Interrupted
            }
            // Check here in case this was set while waiting
            if (exitRequired) {
              return;
            }
          }
          currentNotifies = 0;
        } finally {
          lock.unlock();
        }
        try {
          removeOrphanedPrivileges();
        } catch (Exception e) {
          LOGGER.warn("Privilege cleaning thread encountered an error: " +
                  e.getMessage());
        }
      }
    }

    /**
     * This is called when a privilege is removed from a role.  This may
     * or may not mean that the privilege needs to be removed from the
     * database; there may be more references to it from other roles.
     * As a result, we'll lazily run the orphan cleaner every
     * NOTIFY_THRESHOLD times this routine is called.
     * @param numDeletions The number of potentially orphaned privileges
     */
    public void incPrivRemoval(int numDeletions) {
      if (privCleanerThread != null) {
        try {
          lock.lock();
          currentNotifies += numDeletions;
          if (currentNotifies > NOTIFY_THRESHOLD) {
            cond.signal();
          }
        } finally {
          lock.unlock();
        }
      }
    }

    /**
     * Simple form of incPrivRemoval when only one privilege is deleted.
     */
    public void incPrivRemoval() {
      incPrivRemoval(1);
    }

    /**
     * Tell this thread to exit. Safe to call multiple times, as it just
     * notifies the run() loop to finish up.
     */
    public void exit() {
      if (privCleanerThread != null) {
        lock.lock();
        try {
          exitRequired = true;
          cond.signal();
        } finally {
          lock.unlock();
        }
      }
    }

    /**
     * Run a SQL query to detect orphaned privileges, and then delete
     * each one.  This is complicated by the fact that datanucleus does
     * not seem to play well with the mix between a direct SQL query
     * and operations on the database.  The solution that seems to work
     * is to split the operation into two transactions: the first is
     * just a read for privileges that look like they're orphans, the
     * second transaction will go and get each of those privilege objects,
     * verify that there are no roles attached, and then delete them.
     */
    private void removeOrphanedPrivileges() {
      final String privDB = "SENTRY_DB_PRIVILEGE";
      final String privId = "DB_PRIVILEGE_ID";
      final String mapDB = "SENTRY_ROLE_DB_PRIVILEGE_MAP";
      final String privFilter =
              "select " + privId +
              " from " + privDB + " p" +
              " where not exists (" +
                  " select 1 from " + mapDB + " d" +
                  " where p." + privId + " != d." + privId +
              " )";
      boolean rollback = true;
      int orphansRemoved = 0;
      ArrayList<Object> idList = new ArrayList<Object>();
      PersistenceManager pm = pmf.getPersistenceManager();

      // Transaction 1: Perform a SQL query to get things that look like orphans
      try {
        Transaction transaction = pm.currentTransaction();
        transaction.begin();
        transaction.setRollbackOnly();  // Makes the tx read-only
        Query query = pm.newQuery("javax.jdo.query.SQL", privFilter);
        query.setClass(MSentryPrivilege.class);
        List<MSentryPrivilege> results = (List<MSentryPrivilege>) query.execute();
        for (MSentryPrivilege orphan : results) {
          idList.add(pm.getObjectId(orphan));
        }
        transaction.rollback();
        rollback = false;
      } finally {
        if (rollback && pm.currentTransaction().isActive()) {
          pm.currentTransaction().rollback();
        } else {
          LOGGER.debug("Found {} potential orphans", idList.size());
        }
      }

      if (idList.isEmpty()) {
        pm.close();
        return;
      }

      Preconditions.checkState(!rollback);

      // Transaction 2: For each potential orphan, verify it's really an
      // orphan and delete it if so
      rollback = true;
      try {
        Transaction transaction = pm.currentTransaction();
        transaction.begin();
        pm.refreshAll();  // Try to ensure we really have correct objects
        for (Object id : idList) {
          MSentryPrivilege priv = (MSentryPrivilege) pm.getObjectById(id);
          if (priv.getRoles().isEmpty()) {
            pm.deletePersistent(priv);
            orphansRemoved++;
          }
        }
        transaction.commit();
        pm.close();
        rollback = false;
      } finally {
        if (rollback) {
          rollbackTransaction(pm);
        } else {
          LOGGER.debug("Cleaned up {} orphaned privileges", orphansRemoved);
        }
      }
    }
  }

  // get mapping datas for [group,role], [user,role] with the specific roles
  public List<Map<String, Set<String>>> getGroupUserRoleMapList(final Set<String> roleNames) {
    List<Map<String, Set<String>>> result = new ArrayList<>();
    try {
      result = (List<Map<String, Set<String>>>) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              Query query = pm.newQuery(MSentryRole.class);

              List<String> rolesFiler = new LinkedList<String>();
              if (roleNames != null) {
                for (String rName : roleNames) {
                  rolesFiler.add("(roleName == \"" + rName.trim().toLowerCase() + "\")");
                }
              }
              if (rolesFiler.size() > 0) {
                query.setFilter(Joiner.on(" || ").join(rolesFiler));
              }

              List<MSentryRole> mSentryRoles = (List<MSentryRole>) query.execute();
              Map<String, Set<String>> groupRolesMap = getGroupRolesMap(mSentryRoles);
              Map<String, Set<String>> userRolesMap = getUserRolesMap(mSentryRoles);
              List<Map<String, Set<String>>> mapsList = new ArrayList<>();
              mapsList.add(INDEX_GROUP_ROLES_MAP, groupRolesMap);
              mapsList.add(INDEX_USER_ROLES_MAP, userRolesMap);
              return mapsList;
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  private Map<String, Set<String>> getGroupRolesMap(List<MSentryRole> mSentryRoles) {
    Map<String, Set<String>> groupRolesMap = Maps.newHashMap();
    if (mSentryRoles == null) {
      return groupRolesMap;
    }
    // change the List<MSentryRole> -> Map<groupName, Set<roleName>>
    for (MSentryRole mSentryRole : mSentryRoles) {
      Set<MSentryGroup> groups = mSentryRole.getGroups();
      for (MSentryGroup group : groups) {
        String groupName = group.getGroupName();
        Set<String> rNames = groupRolesMap.get(groupName);
        if (rNames == null) {
          rNames = new HashSet<String>();
        }
        rNames.add(mSentryRole.getRoleName());
        groupRolesMap.put(groupName, rNames);
      }
    }
    return groupRolesMap;
  }

  private Map<String, Set<String>> getUserRolesMap(List<MSentryRole> mSentryRoles) {
    Map<String, Set<String>> userRolesMap = Maps.newHashMap();
    if (mSentryRoles == null) {
      return userRolesMap;
    }
    // change the List<MSentryRole> -> Map<userName, Set<roleName>>
    for (MSentryRole mSentryRole : mSentryRoles) {
      Set<MSentryUser> users = mSentryRole.getUsers();
      for (MSentryUser user : users) {
        String userName = user.getUserName();
        Set<String> rNames = userRolesMap.get(userName);
        if (rNames == null) {
          rNames = new HashSet<String>();
        }
        rNames.add(mSentryRole.getRoleName());
        userRolesMap.put(userName, rNames);
      }
    }
    return userRolesMap;
  }

  // get all mapping data for [role,privilege]
  public Map<String, Set<TSentryPrivilege>> getRoleNameTPrivilegesMap() throws Exception {
    return getRoleNameTPrivilegesMap(null, null);
  }

  // get mapping data for [role,privilege] with the specific auth object
  public Map<String, Set<TSentryPrivilege>> getRoleNameTPrivilegesMap(final String dbName,
        final String tableName) throws Exception {
    return (Map<String, Set<TSentryPrivilege>>) tm.executeTransaction(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            Query query = pm.newQuery(MSentryPrivilege.class);

            List<String> privilegeFiler = new LinkedList<String>();
            if (!StringUtils.isEmpty(dbName)) {
              privilegeFiler.add("(dbName == \"" + dbName.trim().toLowerCase() + "\") ");
            }
            if (!StringUtils.isEmpty(tableName)) {
              privilegeFiler.add("(tableName == \"" + tableName.trim().toLowerCase() + "\") ");
            }
            if (privilegeFiler.size() > 0) {
              query.setFilter(Joiner.on(" && ").join(privilegeFiler));
            }

            List<MSentryPrivilege> mSentryPrivileges = (List<MSentryPrivilege>) query.execute();
            return getRolePrivilegesMap(mSentryPrivileges);
          }
        });
  }

  private Map<String, Set<TSentryPrivilege>> getRolePrivilegesMap(
          List<MSentryPrivilege> mSentryPrivileges) {
    Map<String, Set<TSentryPrivilege>> rolePrivilegesMap = Maps.newHashMap();
    if (mSentryPrivileges == null) {
      return rolePrivilegesMap;
    }
    // change the List<MSentryPrivilege> -> Map<roleName, Set<TSentryPrivilege>>
    for (MSentryPrivilege mSentryPrivilege : mSentryPrivileges) {
      TSentryPrivilege privilege = convertToTSentryPrivilege(mSentryPrivilege);
      for (MSentryRole mSentryRole : mSentryPrivilege.getRoles()) {
        String roleName = mSentryRole.getRoleName();
        Set<TSentryPrivilege> privileges = rolePrivilegesMap.get(roleName);
        if (privileges == null) {
          privileges = new HashSet<TSentryPrivilege>();
        }
        privileges.add(privilege);
        rolePrivilegesMap.put(roleName, privileges);
      }
    }
    return rolePrivilegesMap;
  }

  // Get the all exist role names, will return an empty set
  // if no role names exist.
  public Set<String> getAllRoleNames() {
    Set<String> result = new HashSet<>();
    try {
      result = (Set<String>) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              return getAllRoleNames(pm);
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  // get the all exist role names
  private Set<String> getAllRoleNames(PersistenceManager pm) {
    Query query = pm.newQuery(MSentryRole.class);
    List<MSentryRole> mSentryRoles = (List<MSentryRole>) query.execute();
    Set<String> existRoleNames = Sets.newHashSet();
    if (mSentryRoles != null) {
      for (MSentryRole mSentryRole : mSentryRoles) {
        existRoleNames.add(mSentryRole.getRoleName());
      }
    }
    return existRoleNames;
  }

  // get the all exist groups
  private Map<String, MSentryGroup> getGroupNameTGroupMap(PersistenceManager pm) {
    Query query = pm.newQuery(MSentryGroup.class);
    List<MSentryGroup> mSentryGroups = (List<MSentryGroup>) query.execute();
    Map<String, MSentryGroup> existGroupsMap = Maps.newHashMap();
    if (mSentryGroups != null) {
      // change the List<MSentryGroup> -> Map<groupName, MSentryGroup>
      for (MSentryGroup mSentryGroup : mSentryGroups) {
        existGroupsMap.put(mSentryGroup.getGroupName(), mSentryGroup);
      }
    }
    return existGroupsMap;
  }

  // get the all exist users
  private Map<String, MSentryUser> getUserNameToUserMap(PersistenceManager pm) {
    Query query = pm.newQuery(MSentryUser.class);
    List<MSentryUser> users = (List<MSentryUser>) query.execute();
    Map<String, MSentryUser> existUsersMap = Maps.newHashMap();
    if (users != null) {
      // change the List<MSentryUser> -> Map<userName, MSentryUser>
      for (MSentryUser user : users) {
        existUsersMap.put(user.getUserName(), user);
      }
    }
    return existUsersMap;
  }

  // get the all exist privileges
  private List<MSentryPrivilege> getPrivilegesList(PersistenceManager pm) {
    Query query = pm.newQuery(MSentryPrivilege.class);
    List<MSentryPrivilege> resultList = (List<MSentryPrivilege>) query.execute();
    if (resultList == null) {
      resultList = Lists.newArrayList();
    }
    return resultList;
  }

  @VisibleForTesting
  protected Map<String, MSentryRole> getRolesMap() {
    Map<String, MSentryRole> result = new HashMap<>();
    try {
      result = (Map<String, MSentryRole>) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              Query query = pm.newQuery(MSentryRole.class);
              List<MSentryRole> mSentryRoles = (List<MSentryRole>) query.execute();
              Map<String, MSentryRole> existRolesMap = Maps.newHashMap();
              if (mSentryRoles != null) {
                // change the List<MSentryRole> -> Map<roleName, Set<MSentryRole>>
                for (MSentryRole mSentryRole : mSentryRoles) {
                  existRolesMap.put(mSentryRole.getRoleName(), mSentryRole);
                }
              }

              return existRolesMap;
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  @VisibleForTesting
  protected Map<String, MSentryGroup> getGroupNameToGroupMap() {
    Map<String, MSentryGroup>result = new HashMap<>();
    try {
      result = (Map<String, MSentryGroup>) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              return getGroupNameTGroupMap(pm);
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  @VisibleForTesting
  protected Map<String, MSentryUser> getUserNameToUserMap() {
    Map<String, MSentryUser> result = new HashMap<>();
    try {
      result = (Map<String, MSentryUser>) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              return getUserNameToUserMap(pm);
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  @VisibleForTesting
  protected List<MSentryPrivilege> getPrivilegesList() {
    List<MSentryPrivilege> result = new ArrayList<>();
    try {
      result = (List<MSentryPrivilege>) tm.executeTransaction(
          new TransactionBlock() {
            public Object execute(PersistenceManager pm) throws Exception {
              return getPrivilegesList(pm);
            }
          });
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    return result;
  }

  /**
   * Import the sentry mapping data.
   *
   * @param tSentryMappingData
   *        Include 2 maps to save the mapping data, the following is the example of the data
   *        structure:
   *        for the following mapping data:
   *        user1=role1,role2
   *        user2=role2,role3
   *        group1=role1,role2
   *        group2=role2,role3
   *        role1=server=server1->db=db1
   *        role2=server=server1->db=db1->table=tbl1,server=server1->db=db1->table=tbl2
   *        role3=server=server1->url=hdfs://localhost/path
   *
   *        The GroupRolesMap in TSentryMappingData will be saved as:
   *        {
   *        TSentryGroup(group1)={role1, role2},
   *        TSentryGroup(group2)={role2, role3}
   *        }
   *        The UserRolesMap in TSentryMappingData will be saved as:
   *        {
   *        TSentryUser(user1)={role1, role2},
   *        TSentryGroup(user2)={role2, role3}
   *        }
   *        The RolePrivilegesMap in TSentryMappingData will be saved as:
   *        {
   *        role1={TSentryPrivilege(server=server1->db=db1)},
   *        role2={TSentryPrivilege(server=server1->db=db1->table=tbl1),
   *        TSentryPrivilege(server=server1->db=db1->table=tbl2)},
   *        role3={TSentryPrivilege(server=server1->url=hdfs://localhost/path)}
   *        }
   * @param isOverwriteForRole
   *        The option for merging or overwriting the existing data during import, true for
   *        overwriting, false for merging
   */
  public void importSentryMetaData(final TSentryMappingData tSentryMappingData,
      final boolean isOverwriteForRole) throws Exception {
    tm.executeTransaction(
        new TransactionBlock() {
          public Object execute(PersistenceManager pm) throws Exception {
            // change all role name in lowercase
            TSentryMappingData mappingData = lowercaseRoleName(tSentryMappingData);
            Set<String> existRoleNames = getAllRoleNames(pm);
            //
            Map<String, Set<TSentryGroup>> importedRoleGroupsMap = covertToRoleNameTGroupsMap(mappingData
                .getGroupRolesMap());
            Map<String, Set<String>> importedRoleUsersMap = covertToRoleUsersMap(mappingData
                .getUserRolesMap());
            Set<String> importedRoleNames = importedRoleGroupsMap.keySet();
            // if import with overwrite role, drop the duplicated roles in current DB first.
            if (isOverwriteForRole) {
              dropDuplicatedRoleForImport(pm, existRoleNames, importedRoleNames);
              // refresh the existRoleNames for the drop role
              existRoleNames = getAllRoleNames(pm);
            }

            // import the mapping data for [role,privilege], the existRoleNames will be updated
            importRolePrivilegeMapping(pm, existRoleNames, mappingData.getRolePrivilegesMap());
            // import the mapping data for [role,group], the existRoleNames will be updated
            importRoleGroupMapping(pm, existRoleNames, importedRoleGroupsMap);
            // import the mapping data for [role,user], the existRoleNames will be updated
            importRoleUserMapping(pm, existRoleNames, importedRoleUsersMap);
            return null;
          }
        });
  }

  // covert the Map[group->roles] to Map[role->groups]
  private Map<String, Set<TSentryGroup>> covertToRoleNameTGroupsMap(
      Map<String, Set<String>> groupRolesMap) {
    Map<String, Set<TSentryGroup>> roleGroupsMap = Maps.newHashMap();
    if (groupRolesMap != null) {
      for (Map.Entry<String, Set<String>> entry : groupRolesMap.entrySet()) {
        Set<String> roleNames = entry.getValue();
        if (roleNames != null) {
          for (String roleName : roleNames) {
            Set<TSentryGroup> tSentryGroups = roleGroupsMap.get(roleName);
            if (tSentryGroups == null) {
              tSentryGroups = Sets.newHashSet();
            }
            tSentryGroups.add(new TSentryGroup(entry.getKey()));
            roleGroupsMap.put(roleName, tSentryGroups);
          }
        }
      }
    }
    return roleGroupsMap;
  }

  // covert the Map[user->roles] to Map[role->users]
  private Map<String, Set<String>> covertToRoleUsersMap(
      Map<String, Set<String>> userRolesMap) {
    Map<String, Set<String>> roleUsersMap = Maps.newHashMap();
    if (userRolesMap != null) {
      for (Map.Entry<String, Set<String>> entry : userRolesMap.entrySet()) {
        Set<String> roleNames = entry.getValue();
        if (roleNames != null) {
          for (String roleName : roleNames) {
            Set<String> users = roleUsersMap.get(roleName);
            if (users == null) {
              users = Sets.newHashSet();
            }
            users.add(entry.getKey());
            roleUsersMap.put(roleName, users);
          }
        }
      }
    }
    return roleUsersMap;
  }

  private void importRoleGroupMapping(PersistenceManager pm, Set<String> existRoleNames,
      Map<String, Set<TSentryGroup>> importedRoleGroupsMap) throws Exception {
    if (importedRoleGroupsMap == null || importedRoleGroupsMap.keySet() == null) {
      return;
    }
    for (Map.Entry<String, Set<TSentryGroup>> entry : importedRoleGroupsMap.entrySet()) {
      createRoleIfNotExist(pm, existRoleNames, entry.getKey());
      alterSentryRoleAddGroupsCore(pm, entry.getKey(), entry.getValue());
    }
  }

  private void importRoleUserMapping(PersistenceManager pm, Set<String> existRoleNames,
      Map<String, Set<String>> importedRoleUsersMap) throws Exception {
    if (importedRoleUsersMap == null || importedRoleUsersMap.keySet() == null) {
      return;
    }
    for (Map.Entry<String, Set<String>> entry : importedRoleUsersMap.entrySet()) {
      createRoleIfNotExist(pm, existRoleNames, entry.getKey());
      alterSentryRoleAddUsersCore(pm, entry.getKey(), entry.getValue());
    }
  }

  // drop all duplicated with the imported role
  private void dropDuplicatedRoleForImport(PersistenceManager pm, Set<String> existRoleNames,
      Set<String> importedRoleNames) throws Exception {
    Set<String> duplicatedRoleNames = Sets.intersection(existRoleNames, importedRoleNames);
    for (String droppedRoleName : duplicatedRoleNames) {
      dropSentryRoleCore(pm, droppedRoleName);
    }
  }

  // change all role name in lowercase
  private TSentryMappingData lowercaseRoleName(TSentryMappingData tSentryMappingData) {
    Map<String, Set<String>> sentryGroupRolesMap = tSentryMappingData.getGroupRolesMap();
    Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap = tSentryMappingData
        .getRolePrivilegesMap();

    Map<String, Set<String>> newSentryGroupRolesMap = Maps.newHashMap();
    Map<String, Set<TSentryPrivilege>> newSentryRolePrivilegesMap = Maps.newHashMap();
    // for mapping data [group,role]
    for (Map.Entry<String, Set<String>> entry : sentryGroupRolesMap.entrySet()) {
      Collection<String> lowcaseRoles = Collections2.transform(entry.getValue(),
          new Function<String, String>() {
            @Override
            public String apply(String input) {
              return input.toLowerCase();
            }
          });
      newSentryGroupRolesMap.put(entry.getKey(), Sets.newHashSet(lowcaseRoles));
    }

    // for mapping data [role,privilege]
    for (Map.Entry<String,Set<TSentryPrivilege>> entry : sentryRolePrivilegesMap.entrySet()) {
      newSentryRolePrivilegesMap.put(entry.getKey().toLowerCase(), entry.getValue());
    }

    tSentryMappingData.setGroupRolesMap(newSentryGroupRolesMap);
    tSentryMappingData.setRolePrivilegesMap(newSentryRolePrivilegesMap);
    return tSentryMappingData;
  }

  // import the mapping data for [role,privilege]
  private void importRolePrivilegeMapping(PersistenceManager pm, Set<String> existRoleNames,
      Map<String, Set<TSentryPrivilege>> sentryRolePrivilegesMap) throws Exception {
    if (sentryRolePrivilegesMap != null) {
      for (Map.Entry<String, Set<TSentryPrivilege>> entry : sentryRolePrivilegesMap.entrySet()) {
        // if the rolenName doesn't exist, create it and add it to existRoleNames
        createRoleIfNotExist(pm, existRoleNames, entry.getKey());
        // get the privileges for the role
        Set<TSentryPrivilege> tSentryPrivileges = entry.getValue();
        for (TSentryPrivilege tSentryPrivilege : tSentryPrivileges) {
          alterSentryRoleGrantPrivilegeCore(pm, entry.getKey(), tSentryPrivilege);
        }
      }
    }
  }

  private void createRoleIfNotExist(PersistenceManager pm,
      Set<String> existRoleNames, String roleName) throws Exception {
    String lowerRoleName = trimAndLower(roleName);
    // if the rolenName doesn't exist, create it.
    if (!existRoleNames.contains(lowerRoleName)) {
      createSentryRoleCore(pm, lowerRoleName);
      // update the exist role name set
      existRoleNames.add(lowerRoleName);
    }
  }
}
