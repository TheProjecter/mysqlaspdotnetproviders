//------------------------------------------------------------------------------
// <copyright file="SqlPersonalizationProvider.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

namespace MySql.Providers {
    using System.Web.UI.WebControls.WebParts;
    using System;
    using System.Collections;
    using System.Collections.Specialized;
    using System.Configuration.Provider;
    using System.ComponentModel;
    using System.Data;
    //using System.Data.SqlClient;
    using MySql.Data.MySqlClient; 
    using System.Globalization;
    using System.Security.Permissions;
    using System.Web;
    using System.Web.DataAccess;
    using System.Web.Util;

    // Remove CAS from sample: [AspNetHostingPermission(SecurityAction.LinkDemand, Level=AspNetHostingPermissionLevel.Minimal)]
    // Remove CAS from sample: [AspNetHostingPermission(SecurityAction.InheritanceDemand, Level=AspNetHostingPermissionLevel.Minimal)]
    public class MySqlPersonalizationProvider : PersonalizationProvider {

        static readonly DateTime DefaultInactiveSinceDate = DateTime.MaxValue;
        private enum ResetUserStateMode {
            PerInactiveDate,
            PerPaths,
            PerUsers
        }

        private const int maxStringLength = 256;

        private string _applicationName;
        private int    _commandTimeout;
        private string _connectionString;
        private int    _SchemaVersionCheck;

        /// <devdoc>
        /// Initializes an instance of SqlPersonalizationProvider.
        /// </devdoc>
        public MySqlPersonalizationProvider() {
        }

        public override string ApplicationName {
            get {
                if (String.IsNullOrEmpty(_applicationName)) {
                    _applicationName = SecUtility.GetDefaultAppName();
                }
                return _applicationName;
            }
            set {
                if (value != null && value.Length > maxStringLength) {
                    throw new ProviderException(SR.GetString(
                        SR.PersonalizationProvider_ApplicationNameExceedMaxLength, maxStringLength.ToString(CultureInfo.CurrentCulture)));
                }
                _applicationName = value;
            }
        }

        /// <devdoc>
        /// </devdoc>
        private MySqlParameter CreateParameter(string name, MySqlDbType dbType, object value) {
            MySqlParameter param = new MySqlParameter(name, dbType);

            param.Value = value;
            return param;
        }

        private PersonalizationStateInfoCollection FindSharedState(string path,
                                                                   int pageIndex,
                                                                   int pageSize,
                                                                   out int totalRecords) {
            SqlConnectionHolder connectionHolder = null;
            MySqlConnection connection = null;
            MySqlDataReader reader = null;
            totalRecords = 0;

            try {
                try {
                    connectionHolder = GetConnectionHolder();
                    connection = connectionHolder.Connection;

                    CheckSchemaVersion( connection );

                    MySqlCommand command = new MySqlCommand(); //"dbo.aspnet_PersonalizationAdministration_FindState", connection);

                    totalRecords = MySqlStoredProcedures.aspnet_PersonalizationAdministration_FindState(true,
                            ApplicationName, pageIndex, pageSize, path, null, new DateTime(1754, 1, 1, 0, 0, 0), connectionHolder, ref command);

                    SetCommandTypeAndTimeout(command);
                    //MySqlParameterCollection parameters = command.Parameters;

                    //MySqlParameter parameter = parameters.Add(new MySqlParameter("AllUsersScope", MySqlDbType.Bit));
                    //parameter.Value = true;

                    //parameters.AddWithValue("ApplicationName", ApplicationName);
                    //parameters.AddWithValue("PageIndex", pageIndex);
                    //parameters.AddWithValue("PageSize", pageSize);

                    //MySqlParameter returnValue = new MySqlParameter("@ReturnValue", MySqlDbType.Int32);
                    //returnValue.Direction = ParameterDirection.ReturnValue;
                    //parameters.Add(returnValue);

                    //parameter = parameters.Add("Path", MySqlDbType.VarChar);
                    //if (path != null) {
                    //    parameter.Value = path;
                    //}

                    //parameter = parameters.Add("UserName", MySqlDbType.VarChar);
                    //parameter = parameters.Add("InactiveSinceDate", MySqlDbType.DateTime);

                    reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
                    PersonalizationStateInfoCollection sharedStateInfoCollection = new PersonalizationStateInfoCollection();

                    if (reader != null) {
                        if (reader.HasRows) {
                            while(reader.Read()) {
                                string returnedPath = reader.GetString(0);

                                // Data can be null if there is no data associated with the path
                                DateTime lastUpdatedDate = (reader.IsDBNull(1)) ? DateTime.MinValue :
                                                                DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
                                int size = (reader.IsDBNull(2)) ? 0 : reader.GetInt32(2);
                                int userDataSize = (reader.IsDBNull(3)) ? 0 : reader.GetInt32(3);
                                int userCount = (reader.IsDBNull(4)) ? 0 : reader.GetInt32(4);
                                sharedStateInfoCollection.Add(new SharedPersonalizationStateInfo(
                                    returnedPath, lastUpdatedDate, size, userDataSize, userCount));
                            }
                        }

                        // The reader needs to be closed so return value can be accessed
                        // See MSDN doc for MySqlParameter.Direction for details.
                        reader.Close();
                        reader = null;
                    }

                    // Set the total count at the end after all operations pass
                    //if (returnValue.Value != null && returnValue.Value is int) {
                    //    totalRecords = (int)returnValue.Value;
                    //}

                    return sharedStateInfoCollection;
                }
                finally {
                    if (reader != null) {
                        reader.Close();
                    }

                    if (connectionHolder != null) {
                        connectionHolder.Close();
                        connectionHolder = null;
                    }
                }
            }
            catch {
                throw;
            }
        }

        public override PersonalizationStateInfoCollection FindState(PersonalizationScope scope,
                                                                     PersonalizationStateQuery query,
                                                                     int pageIndex, int pageSize,
                                                                     out int totalRecords) {
            PersonalizationProviderHelper.CheckPersonalizationScope(scope);
            PersonalizationProviderHelper.CheckPageIndexAndSize(pageIndex, pageSize);

            if (scope == PersonalizationScope.Shared) {
                string pathToMatch = null;
                if (query != null) {
                    pathToMatch = CheckAndTrimString(query.PathToMatch, "query.PathToMatch", false, maxStringLength);
                }
                return FindSharedState(pathToMatch, pageIndex, pageSize, out totalRecords);
            }
            else {
                string pathToMatch = null;
                DateTime inactiveSinceDate = DefaultInactiveSinceDate;
                string usernameToMatch = null;
                if (query != null) {
                    pathToMatch = CheckAndTrimString(query.PathToMatch, "query.PathToMatch", false, maxStringLength);
                    inactiveSinceDate = query.UserInactiveSinceDate;
                    usernameToMatch = CheckAndTrimString(query.UsernameToMatch, "query.UsernameToMatch", false, maxStringLength);
                }

                return FindUserState(pathToMatch, inactiveSinceDate, usernameToMatch,
                                     pageIndex, pageSize, out totalRecords);
            }
        }

        private PersonalizationStateInfoCollection FindUserState(string path,
                                                                 DateTime inactiveSinceDate,
                                                                 string username,
                                                                 int pageIndex,
                                                                 int pageSize,
                                                                 out int totalRecords) {
            SqlConnectionHolder connectionHolder = null;
            MySqlConnection connection = null;
            MySqlDataReader reader = null;
            totalRecords = 0;

            try {
                try {
                    connectionHolder = GetConnectionHolder();
                    connection = connectionHolder.Connection;

                    CheckSchemaVersion( connection );

                    MySqlCommand command = new MySqlCommand(); //"dbo.aspnet_PersonalizationAdministration_FindState", connection);

                    totalRecords = MySqlStoredProcedures.aspnet_PersonalizationAdministration_FindState(false,
                            ApplicationName, pageIndex, pageSize, path, username, inactiveSinceDate, connectionHolder,
                            ref command);
                    
                    SetCommandTypeAndTimeout(command);
                    //MySqlParameterCollection parameters = command.Parameters;

                    //MySqlParameter parameter = parameters.Add(new MySqlParameter("AllUsersScope", MySqlDbType.Bit));
                    //parameter.Value = false;

                    //parameters.AddWithValue("ApplicationName", ApplicationName);
                    //parameters.AddWithValue("PageIndex", pageIndex);
                    //parameters.AddWithValue("PageSize", pageSize);

                    //MySqlParameter returnValue = new MySqlParameter("@ReturnValue", MySqlDbType.Int32);
                    //returnValue.Direction = ParameterDirection.ReturnValue;
                    //parameters.Add(returnValue);

                    //parameter = parameters.Add("Path", MySqlDbType.VarChar);
                    //if (path != null) {
                    //    parameter.Value = path;
                    //}

                    //parameter = parameters.Add("UserName", MySqlDbType.VarChar);
                    //if (username != null) {
                    //    parameter.Value = username;
                    //}

                    //parameter = parameters.Add("InactiveSinceDate", MySqlDbType.DateTime);
                    //if (inactiveSinceDate != DefaultInactiveSinceDate) {
                    //    parameter.Value = inactiveSinceDate.ToUniversalTime();
                    //}

                    reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
                    PersonalizationStateInfoCollection stateInfoCollection = new PersonalizationStateInfoCollection();

                    if (reader != null) {
                        if (reader.HasRows) {
                            while(reader.Read()) {
                                string returnedPath = reader.GetString(0);
                                DateTime lastUpdatedDate = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
                                int size = reader.GetInt32(2);
                                string returnedUsername = reader.GetString(3);
                                DateTime lastActivityDate = DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc);
                                stateInfoCollection.Add(new UserPersonalizationStateInfo(
                                                                returnedPath, lastUpdatedDate,
                                                                size, returnedUsername, lastActivityDate));
                            }
                        }

                        // The reader needs to be closed so return value can be accessed
                        // See MSDN doc for MySqlParameter.Direction for details.
                        reader.Close();
                        reader = null;
                    }

                    // Set the total count at the end after all operations pass
                    //if (returnValue.Value != null && returnValue.Value is int) {
                    //    totalRecords = (int)returnValue.Value;
                    //}

                    return stateInfoCollection;
                }
                finally {
                    if (reader != null) {
                        reader.Close();
                    }

                    if (connectionHolder != null) {
                        connectionHolder.Close();
                        connectionHolder = null;
                    }
                }
            }
            catch {
                throw;
            }
        }

        /// <devdoc>
        /// </devdoc>
        private SqlConnectionHolder GetConnectionHolder() {
            MySqlConnection connection = null;
            SqlConnectionHolder connectionHolder = SqlConnectionHelper.GetConnection(_connectionString, true);

            if (connectionHolder != null) {
                connection = connectionHolder.Connection;
            }
            if (connection == null) {
                throw new ProviderException(SR.GetString(SR.PersonalizationProvider_CantAccess, Name));
            }

            return connectionHolder;
        }

        private int GetCountOfSharedState(string path) {
            SqlConnectionHolder connectionHolder = null;
            MySqlConnection connection = null;
            int count = 0;

            try {
                try {
                    connectionHolder = GetConnectionHolder();
                    connection = connectionHolder.Connection;

                    CheckSchemaVersion( connection );

                    //MySqlCommand command = new MySqlCommand("dbo.aspnet_PersonalizationAdministration_GetCountOfState", connection);
                    
                    //SetCommandTypeAndTimeout(command);
                    //MySqlParameterCollection parameters = command.Parameters;

                    //MySqlParameter parameter = parameters.Add(new MySqlParameter("Count", MySqlDbType.Int32));
                    //parameter.Direction = ParameterDirection.Output;

                    //parameter = parameters.Add(new MySqlParameter("AllUsersScope", MySqlDbType.Bit));
                    //parameter.Value = true;

                    //parameters.AddWithValue("ApplicationName", ApplicationName);

                    //parameter = parameters.Add("Path", MySqlDbType.VarChar);
                    //if (path != null) {
                    //    parameter.Value = path;
                    //}

                    //parameter = parameters.Add("UserName", MySqlDbType.VarChar);
                    //parameter = parameters.Add("InactiveSinceDate", MySqlDbType.DateTime);

                    //command.ExecuteNonQuery();

                    count = MySqlStoredProcedures.aspnet_PersonalizationAdministration_GetCountOfState(
                            true, ApplicationName, path, null, new DateTime(1754, 1, 1, 0, 0, 0), connectionHolder);

                    //parameter = command.Parameters[0];
                    //if (parameter != null && parameter.Value != null && parameter.Value is Int32) {
                    //    count = (Int32) parameter.Value;
                    //}
                }
                finally {
                    if (connectionHolder != null) {
                        connectionHolder.Close();
                        connectionHolder = null;
                    }
                }
            }
            catch {
                throw;
            }

            return count;
        }

        public override int GetCountOfState(PersonalizationScope scope, PersonalizationStateQuery query) {
            PersonalizationProviderHelper.CheckPersonalizationScope(scope);
            if (scope == PersonalizationScope.Shared) {
                string pathToMatch = null;
                if (query != null) {
                    pathToMatch = CheckAndTrimString(query.PathToMatch, "query.PathToMatch", false, maxStringLength);
                }
                return GetCountOfSharedState(pathToMatch);
            }
            else {
                string pathToMatch = null;
                DateTime userInactiveSinceDate = DefaultInactiveSinceDate;
                string usernameToMatch = null;
                if (query != null) {
                    pathToMatch = CheckAndTrimString(query.PathToMatch, "query.PathToMatch", false, maxStringLength);
                    userInactiveSinceDate = query.UserInactiveSinceDate;
                    usernameToMatch = CheckAndTrimString(query.UsernameToMatch, "query.UsernameToMatch", false, maxStringLength);
                }
                return GetCountOfUserState(pathToMatch, userInactiveSinceDate, usernameToMatch);
            }
        }

        private int GetCountOfUserState(string path, DateTime inactiveSinceDate, string username) {
            SqlConnectionHolder connectionHolder = null;
            MySqlConnection connection = null;
            int count = 0;

            try {
                try {
                    connectionHolder = GetConnectionHolder();
                    connection = connectionHolder.Connection;

                    CheckSchemaVersion( connection );

                    //MySqlCommand command = new MySqlCommand("dbo.aspnet_PersonalizationAdministration_GetCountOfState", connection);
                    //SetCommandTypeAndTimeout(command);
                    //MySqlParameterCollection parameters = command.Parameters;

                    //MySqlParameter parameter = parameters.Add(new MySqlParameter("Count", MySqlDbType.Int32));
                    //parameter.Direction = ParameterDirection.Output;

                    //parameter = parameters.Add(new MySqlParameter("AllUsersScope", MySqlDbType.Bit));
                    //parameter.Value = false;

                    //parameters.AddWithValue("ApplicationName", ApplicationName);

                    //parameter = parameters.Add("Path", MySqlDbType.VarChar);
                    //if (path != null) {
                    //    parameter.Value = path;
                    //}

                    //parameter = parameters.Add("UserName", MySqlDbType.VarChar);
                    //if (username != null) {
                    //    parameter.Value = username;
                    //}

                    //parameter = parameters.Add("InactiveSinceDate", MySqlDbType.DateTime);
                    //if (inactiveSinceDate != DefaultInactiveSinceDate) {
                    //    parameter.Value = inactiveSinceDate.ToUniversalTime();
                    //}

                    //command.ExecuteNonQuery();
                    //parameter = command.Parameters[0];
                    //if (parameter != null && parameter.Value != null && parameter.Value is Int32) {
                    //    count = (Int32) parameter.Value;
                    //}
                    count = MySqlStoredProcedures.aspnet_PersonalizationAdministration_GetCountOfState(false,
                            ApplicationName, path, username, inactiveSinceDate, connectionHolder);
                }
                finally {
                    if (connectionHolder != null) {
                        connectionHolder.Close();
                        connectionHolder = null;
                    }
                }
            }
            catch {
                throw;
            }

            return count;
        }

        public override void Initialize(string name, NameValueCollection configSettings) {
            // Remove CAS in sample: HttpRuntime.CheckAspNetHostingPermission(AspNetHostingPermissionLevel.Low, SR.Feature_not_supported_at_this_level);

            // configSettings cannot be null because there are required settings needed below
            if (configSettings == null) {
                throw new ArgumentNullException("configSettings");
            }

            if (String.IsNullOrEmpty(name)) {
                name = "SqlPersonalizationProvider";
            }

            // description will be set from the base class' Initialize method
            if (string.IsNullOrEmpty(configSettings["description"])) {
                configSettings.Remove("description");
                configSettings.Add("description", SR.GetString(SR.SqlPersonalizationProvider_Description));
            }
            base.Initialize(name, configSettings);

            _SchemaVersionCheck = 0;

            // If not available, the default value is set in the get accessor of ApplicationName
            _applicationName = configSettings["applicationName"];
            if (_applicationName != null) {
                configSettings.Remove("applicationName");

                if (_applicationName.Length > maxStringLength) {
                    throw new ProviderException(SR.GetString(
                        SR.PersonalizationProvider_ApplicationNameExceedMaxLength, maxStringLength.ToString(CultureInfo.CurrentCulture)));
                }
            }

            string connectionStringName = configSettings["connectionStringName"];
            if (String.IsNullOrEmpty(connectionStringName)) {
                throw new ProviderException(SR.GetString(SR.PersonalizationProvider_NoConnection));
            }
            configSettings.Remove("connectionStringName");

            string connectionString = SqlConnectionHelper.GetConnectionString(connectionStringName, true, true);
            if (String.IsNullOrEmpty(connectionString)) {
                throw new ProviderException(SR.GetString(SR.PersonalizationProvider_BadConnection, connectionStringName));
            }
            _connectionString = connectionString;

            _commandTimeout = SecUtility.GetIntValue(configSettings, "commandTimeout", -1, true, 0);
            configSettings.Remove("commandTimeout");

            if (configSettings.Count > 0) {
                string invalidAttributeName = configSettings.GetKey(0);
                throw new ProviderException(SR.GetString(SR.PersonalizationProvider_UnknownProp, invalidAttributeName, name));
            }
        }

        private void CheckSchemaVersion( MySqlConnection connection )
        {
            string[] features = { "Personalization" };
            string   version  = "1";

            SecUtility.CheckSchemaVersion( this,
                                           connection,
                                           features,
                                           version,
                                           ref _SchemaVersionCheck );
        }

        /// <devdoc>
        /// </devdoc>
        private byte[] LoadPersonalizationBlob(SqlConnectionHolder connectionHolder, string path, string userName) {
            MySqlCommand command = new MySqlCommand();

            int status;

            if (userName != null) {
                status = MySqlStoredProcedures.aspnet_PersonalizationPerUser_GetPageSettings(ApplicationName,
                        userName, path, DateTime.UtcNow, connectionHolder, ref command);

                //command = new MySqlCommand("dbo.aspnet_PersonalizationPerUser_GetPageSettings", connection);
            }
            else {
                status = MySqlStoredProcedures.aspnet_PersonalizationAllUsers_GetPageSettings(ApplicationName,
                        path, connectionHolder, ref command);

                //command = new MySqlCommand("dbo.aspnet_PersonalizationAllUsers_GetPageSettings", connection);
            }

            //SetCommandTypeAndTimeout(command);
            //command.Parameters.Add(CreateParameter("@ApplicationName", MySqlDbType.VarChar, this.ApplicationName));
            //command.Parameters.Add(CreateParameter("@Path", MySqlDbType.VarChar, path));
            //if (userName != null) {
            //    command.Parameters.Add(CreateParameter("@UserName", MySqlDbType.VarChar, userName));
            //    command.Parameters.Add(CreateParameter("@CurrentTimeUtc", MySqlDbType.DateTime, DateTime.UtcNow));
            //}

            MySqlDataReader reader = null;
            try {
                reader = command.ExecuteReader(CommandBehavior.SingleRow);
                if (reader.Read()) {
                    int length = (int)reader.GetBytes(0, 0, null, 0, 0);
                    byte[] state = new byte[length];

                    reader.GetBytes(0, 0, state, 0, length);
                    return state;
                }
            }
            finally {
                if (reader != null) {
                    reader.Close();
                }
            }

            return null;
        }

        /// <internalonly />
        protected override void LoadPersonalizationBlobs(WebPartManager webPartManager, string path, string userName, ref byte[] sharedDataBlob, ref byte[] userDataBlob) {
            sharedDataBlob = null;
            userDataBlob = null;

            SqlConnectionHolder connectionHolder = null;
            MySqlConnection connection = null;

            try {
                try {
                    connectionHolder = GetConnectionHolder();
                    connection = connectionHolder.Connection;

                    CheckSchemaVersion( connection );

                    sharedDataBlob = LoadPersonalizationBlob(connectionHolder, path, null);
                    if (!String.IsNullOrEmpty(userName)) {
                        userDataBlob = LoadPersonalizationBlob(connectionHolder, path, userName);
                    }
                }
                finally {
                    if (connectionHolder != null) {
                        connectionHolder.Close();
                        connectionHolder = null;
                    }
                }
            }
            catch {
                throw;
            }
        }

        /// <devdoc>
        /// </devdoc>
        private void ResetPersonalizationState(SqlConnectionHolder connectionHolder, string path, string userName) {

            //MySqlCommand command;
            int status;

            if (userName != null) {
                status = MySqlStoredProcedures.aspnet_PersonalizationPerUser_ResetPageSettings(ApplicationName,
                        userName, path, DateTime.UtcNow, connectionHolder);

                //command = new MySqlCommand("dbo.aspnet_PersonalizationPerUser_ResetPageSettings", connection);
            }
            else {
                status = MySqlStoredProcedures.aspnet_PersonalizationAllUsers_ResetPageSettings(ApplicationName,
                        path, connectionHolder);

                //command = new MySqlCommand("dbo.aspnet_PersonalizationAllUsers_ResetPageSettings", connection);
            }

            //SetCommandTypeAndTimeout(command);
            //command.Parameters.Add(CreateParameter("@ApplicationName", MySqlDbType.VarChar, ApplicationName));
            //command.Parameters.Add(CreateParameter("@Path", MySqlDbType.VarChar, path));
            //if (userName != null) {
            //    command.Parameters.Add(CreateParameter("@UserName", MySqlDbType.VarChar, userName));
            //    command.Parameters.Add(CreateParameter("@CurrentTimeUtc", MySqlDbType.DateTime, DateTime.UtcNow));
            //}

            //command.ExecuteNonQuery();
        }

        /// <internalonly />
        protected override void ResetPersonalizationBlob(WebPartManager webPartManager, string path, string userName) {
            SqlConnectionHolder connectionHolder = null;
            MySqlConnection connection = null;

            try {
                try {
                    connectionHolder = GetConnectionHolder();
                    connection = connectionHolder.Connection;

                    CheckSchemaVersion( connection );

                    ResetPersonalizationState(connectionHolder, path, userName);
                }
                finally {
                    if (connectionHolder != null) {
                        connectionHolder.Close();
                        connectionHolder = null;
                    }
                }
            }
            catch {
                throw;
            }
        }

        private int ResetAllState(PersonalizationScope scope) {
            SqlConnectionHolder connectionHolder = null;
            MySqlConnection connection = null;
            int count = 0;

            try {
                try {
                    connectionHolder = GetConnectionHolder();
                    connection = connectionHolder.Connection;

                    CheckSchemaVersion( connection );

                    //MySqlCommand command = new MySqlCommand("dbo.aspnet_PersonalizationAdministration_DeleteAllState", connection);
                    //SetCommandTypeAndTimeout(command);
                    //MySqlParameterCollection parameters = command.Parameters;

                    //MySqlParameter parameter = parameters.Add(new MySqlParameter("AllUsersScope", MySqlDbType.Bit));
                    //parameter.Value = (scope == PersonalizationScope.Shared);

                    //parameters.AddWithValue("ApplicationName", ApplicationName);

                    //parameter = parameters.Add(new MySqlParameter("Count", MySqlDbType.Int32));
                    //parameter.Direction = ParameterDirection.Output;

                    //command.ExecuteNonQuery();
                    //parameter = command.Parameters[2];
                    //if (parameter != null && parameter.Value != null && parameter.Value is Int32) {
                    //    count = (Int32) parameter.Value;
                    //}

                    count = MySqlStoredProcedures.aspnet_PersonalizationAdministration_DeleteAllState(
                        scope == PersonalizationScope.Shared, ApplicationName, connectionHolder);
                }
                finally {
                    if (connectionHolder != null) {
                        connectionHolder.Close();
                        connectionHolder = null;
                    }
                }
            }
            catch {
                throw;
            }

            return count;
        }

        private int ResetSharedState(string[] paths) {
            int resultCount = 0;

            if (paths == null) {
                resultCount = ResetAllState(PersonalizationScope.Shared);
            }
            else {
                SqlConnectionHolder connectionHolder = null;
                MySqlConnection connection = null;

                try {
                    //bool beginTranCalled = false;
                    try {
                        connectionHolder = GetConnectionHolder();
                        connection = connectionHolder.Connection;

                        CheckSchemaVersion( connection );

                        resultCount = MySqlStoredProcedures.aspnet_PersonalizationAdministration_ResetSharedState(
                                ApplicationName, paths, connectionHolder);

                    //    MySqlCommand command = new MySqlCommand("dbo.aspnet_PersonalizationAdministration_ResetSharedState", connection);
                    //    SetCommandTypeAndTimeout(command);
                    //    MySqlParameterCollection parameters = command.Parameters;

                    //    MySqlParameter parameter = parameters.Add(new MySqlParameter("Count", MySqlDbType.Int32));
                    //    parameter.Direction = ParameterDirection.Output;

                    //    parameters.AddWithValue("ApplicationName", ApplicationName);

                    //    //
                    //    // Note:  ADO.NET 2.0 introduced the TransactionScope class - in your own code you should use TransactionScope
                    //    //            rather than explicitly managing transactions with the TSQL BEGIN/COMMIT/ROLLBACK statements.
                    //    //
                    //    parameter = parameters.Add("Path", MySqlDbType.VarChar);
                    //    foreach (string path in paths) {
                    //        if (!beginTranCalled && paths.Length > 1) {
                    //            (new MySqlCommand("BEGIN TRANSACTION", connection)).ExecuteNonQuery();
                    //            beginTranCalled = true;
                    //        }

                    //        parameter.Value = path;
                    //        command.ExecuteNonQuery();
                    //        MySqlParameter countParam = command.Parameters[0];
                    //        if (countParam != null && countParam.Value != null && countParam.Value is Int32) {
                    //            resultCount += (Int32) countParam.Value;
                    //        }
                    //    }

                    //    if (beginTranCalled) {
                    //        (new MySqlCommand("COMMIT TRANSACTION", connection)).ExecuteNonQuery();
                    //        beginTranCalled = false;
                    //    }
                    //}
                    //catch {
                    //    if (beginTranCalled) {
                    //        (new MySqlCommand("ROLLBACK TRANSACTION", connection)).ExecuteNonQuery();
                    //        beginTranCalled = false;
                    //    }
                    //    throw;
                    }
                    finally {
                       if (connectionHolder != null) {
                            connectionHolder.Close();
                            connectionHolder = null;
                        }
                    }
                }
                catch {
                    throw;
                }
            }

            return resultCount;
        }

        public override int ResetUserState(string path, DateTime userInactiveSinceDate) {
            path = CheckAndTrimString(path, "path", false, maxStringLength);
            string [] paths = (path == null) ? null : new string [] {path};
            return ResetUserState(ResetUserStateMode.PerInactiveDate,
                                  userInactiveSinceDate, paths, null);
        }

        public override int ResetState(PersonalizationScope scope, string[] paths, string[] usernames) {
            PersonalizationProviderHelper.CheckPersonalizationScope(scope);
            paths = PersonalizationProviderHelper.CheckAndTrimNonEmptyStringEntries(paths, "paths", false, false, maxStringLength);
            usernames = PersonalizationProviderHelper.CheckAndTrimNonEmptyStringEntries(usernames, "usernames", false, true, maxStringLength);

            if (scope == PersonalizationScope.Shared) {
                PersonalizationProviderHelper.CheckUsernamesInSharedScope(usernames);
                return ResetSharedState(paths);
            }
            else {
                PersonalizationProviderHelper.CheckOnlyOnePathWithUsers(paths, usernames);
                return ResetUserState(paths, usernames);
            }
        }

        private int ResetUserState(string[] paths, string[] usernames) {
            int count = 0;
            bool hasPaths = !(paths == null || paths.Length == 0);
            bool hasUsernames = !(usernames == null || usernames.Length == 0);

            if (!hasPaths && !hasUsernames) {
                count = ResetAllState(PersonalizationScope.User);
            }
            else if (!hasUsernames) {
                count = ResetUserState(ResetUserStateMode.PerPaths,
                                       DefaultInactiveSinceDate,
                                       paths, usernames);
            }
            else {
                count = ResetUserState(ResetUserStateMode.PerUsers,
                                       DefaultInactiveSinceDate,
                                       paths, usernames);
            }

            return count;
        }

        private int ResetUserState(ResetUserStateMode mode,
                                   DateTime userInactiveSinceDate,
                                   string[] paths,
                                   string[] usernames) {
            SqlConnectionHolder connectionHolder = null;
            MySqlConnection connection = null;
            int count = 0;

            try {
                //bool beginTranCalled = false;
                try {
                    connectionHolder = GetConnectionHolder();
                    connection = connectionHolder.Connection;

                    CheckSchemaVersion( connection );

                    if (mode == ResetUserStateMode.PerInactiveDate) 
                    {
                        count = MySqlStoredProcedures.aspnet_PersonalizationAdministration_ResetUserState(
                            ApplicationName, userInactiveSinceDate.ToUniversalTime(), null, null,
                            connectionHolder);
                    }
                    else if (mode == ResetUserStateMode.PerPaths)
                    {
                        count = MySqlStoredProcedures.aspnet_PersonalizationAdministration_ResetUserState(
                            ApplicationName, new DateTime(1754, 1, 1, 0, 0, 0), null, paths,
                            connectionHolder);
                    }
                    else
                    {
                        count = MySqlStoredProcedures.aspnet_PersonalizationAdministration_ResetUserState(
                            ApplicationName, new DateTime(1754, 1, 1, 0, 0, 0), usernames, null,
                            connectionHolder);
                    }
                    
                    //MySqlCommand command = new MySqlCommand("dbo.aspnet_PersonalizationAdministration_ResetUserState", connection);
                    //SetCommandTypeAndTimeout(command);
                    //MySqlParameterCollection parameters = command.Parameters;

                    //MySqlParameter parameter = parameters.Add(new MySqlParameter("Count", MySqlDbType.Int32));
                    //parameter.Direction = ParameterDirection.Output;

                    //parameters.AddWithValue("ApplicationName", ApplicationName);

                    //string firstPath = (paths != null && paths.Length > 0) ? paths[0] : null;

                    //if (mode == ResetUserStateMode.PerInactiveDate) {
                    //    if (userInactiveSinceDate != DefaultInactiveSinceDate) {
                    //        // Special note: DateTime object cannot be added to collection
                    //        // via AddWithValue for some reason.
                    //        parameter = parameters.Add("InactiveSinceDate", MySqlDbType.DateTime);
                    //        parameter.Value = userInactiveSinceDate.ToUniversalTime();
                    //    }

                    //    if (firstPath != null) {
                    //        parameters.AddWithValue("Path", firstPath);
                    //    }

                    //    command.ExecuteNonQuery();
                    //    MySqlParameter countParam = command.Parameters[0];
                    //    if (countParam != null && countParam.Value != null && countParam.Value is Int32) {
                    //        count = (Int32) countParam.Value;
                    //    }
                    //}
                    //else if (mode == ResetUserStateMode.PerPaths) {
                    //    parameter = parameters.Add("Path", MySqlDbType.VarChar);
                    //    foreach (string path in paths) {
                    //        //
                    //        // Note:  ADO.NET 2.0 introduced the TransactionScope class - in your own code you should use TransactionScope
                    //        //            rather than explicitly managing transactions with the TSQL BEGIN/COMMIT/ROLLBACK statements.
                    //        //
                    //        if (!beginTranCalled && paths.Length > 1) {
                    //            (new MySqlCommand("BEGIN TRANSACTION", connection)).ExecuteNonQuery();
                    //            beginTranCalled = true;
                    //        }

                    //        parameter.Value = path;
                    //        command.ExecuteNonQuery();
                    //        MySqlParameter countParam = command.Parameters[0];
                    //        if (countParam != null && countParam.Value != null && countParam.Value is Int32) {
                    //            count += (Int32) countParam.Value;
                    //        }
                    //    }
                    //}
                    //else {
                    //    if (firstPath != null) {
                    //        parameters.AddWithValue("Path", firstPath);
                    //    }

                    //    parameter = parameters.Add("UserName", MySqlDbType.VarChar);
                    //    foreach (string user in usernames) {
                    //        //
                    //        // Note:  ADO.NET 2.0 introduced the TransactionScope class - in your own code you should use TransactionScope
                    //        //            rather than explicitly managing transactions with the TSQL BEGIN/COMMIT/ROLLBACK statements.
                    //        //
                    //        if (!beginTranCalled && usernames.Length > 1) {
                    //            (new MySqlCommand("BEGIN TRANSACTION", connection)).ExecuteNonQuery();
                    //            beginTranCalled = true;
                    //        }

                    //        parameter.Value = user;
                    //        command.ExecuteNonQuery();
                    //        MySqlParameter countParam = command.Parameters[0];
                    //        if (countParam != null && countParam.Value != null && countParam.Value is Int32) {
                    //            count += (Int32) countParam.Value;
                    //        }
                    //    }
                    //}

                    //if (beginTranCalled) {
                    //    (new MySqlCommand("COMMIT TRANSACTION", connection)).ExecuteNonQuery();
                    //    beginTranCalled = false;
                    //}
                }
                //catch {
                //    if (beginTranCalled) {
                //        (new MySqlCommand("ROLLBACK TRANSACTION", connection)).ExecuteNonQuery();
                //        beginTranCalled = false;
                //    }
                //    throw;
                //}
                finally {
                    if (connectionHolder != null) {
                        connectionHolder.Close();
                        connectionHolder = null;
                    }
                }
            }
            catch {
                throw;
            }

            return count;
        }

        /// <devdoc>
        /// </devdoc>
        private void SavePersonalizationState(SqlConnectionHolder connectionHolder, string path, string userName, byte[] state) {
            //MySqlCommand command;
            int count;

            if (userName != null) {
                count = MySqlStoredProcedures.aspnet_PersonalizationPerUser_SetPageSettings(ApplicationName,
                        userName, path, state, DateTime.UtcNow, connectionHolder);
                //command = new MySqlCommand("dbo.aspnet_PersonalizationPerUser_SetPageSettings", connection);
            }
            else {
                count = MySqlStoredProcedures.aspnet_PersonalizationAllUsers_SetPageSettings(ApplicationName,
                        path, state, DateTime.UtcNow, connectionHolder);

                //command = new MySqlCommand("dbo.aspnet_PersonalizationAllUsers_SetPageSettings", connection);
            }

            //SetCommandTypeAndTimeout(command);
            //command.Parameters.Add(CreateParameter("@ApplicationName", MySqlDbType.VarChar, ApplicationName));
            //command.Parameters.Add(CreateParameter("@Path", MySqlDbType.VarChar, path));
            //command.Parameters.Add(CreateParameter("@PageSettings", MySqlDbType.Blob, state));
            //command.Parameters.Add(CreateParameter("@CurrentTimeUtc", MySqlDbType.DateTime, DateTime.UtcNow));
            //if (userName != null) {
            //    command.Parameters.Add(CreateParameter("@UserName", MySqlDbType.VarChar, userName));
            //}

            //command.ExecuteNonQuery();
        }

        /// <internalonly />
        protected override void SavePersonalizationBlob(WebPartManager webPartManager, string path, string userName, byte[] dataBlob) {
            SqlConnectionHolder connectionHolder = null;
            MySqlConnection connection = null;

            try {
                try {
                    connectionHolder = GetConnectionHolder();
                    connection = connectionHolder.Connection;

                    CheckSchemaVersion( connection );

                    SavePersonalizationState(connectionHolder, path, userName, dataBlob);
                }
                finally {
                    if (connectionHolder != null) {
                        connectionHolder.Close();
                        connectionHolder = null;
                    }
                }
            }
            catch {
                throw;
            }
        }

        private void SetCommandTypeAndTimeout(MySqlCommand command) {
            command.CommandType = CommandType.StoredProcedure;
            if (_commandTimeout != -1) {
                command.CommandTimeout = _commandTimeout;
            }
        }

        internal static string CheckAndTrimString(string paramValue, string paramName) {
            return CheckAndTrimString(paramValue, paramName, true);
        }

        internal static string CheckAndTrimString(string paramValue, string paramName, bool throwIfNull) {
            return CheckAndTrimString(paramValue, paramName, throwIfNull, -1);
        }

        internal static string CheckAndTrimString(string paramValue, string paramName,
                                                  bool throwIfNull, int lengthToCheck) {
            if (paramValue == null) {
                if (throwIfNull) {
                    throw new ArgumentNullException(paramName);
                }
                return null;
            }
            string trimmedValue = paramValue.Trim();
            if (trimmedValue.Length == 0) {
                throw new ArgumentException(
                        SR.GetString(SR.PersonalizationProviderHelper_TrimmedEmptyString,
                                     paramName));
            }
            if (lengthToCheck > -1 && trimmedValue.Length > lengthToCheck) {
                throw new ArgumentException(
                        SR.GetString(SR.StringUtil_Trimmed_String_Exceed_Maximum_Length,
                                     paramValue, paramName, lengthToCheck.ToString(CultureInfo.InvariantCulture)));
            }
            return trimmedValue;
        }
    }
}
