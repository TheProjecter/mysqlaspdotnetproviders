//------------------------------------------------------------------------------
// <copyright file="SqlMembershipProvider.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

namespace MySql.Providers {
    using  System;
    using  System.Web.Security;
    using  System.Web;
    using  System.Web.Configuration;
    using  System.Security.Principal;
    using  System.Security.Permissions;
    using  System.Globalization;
    using  System.Runtime.Serialization;
    using  System.Collections;
    using  System.Collections.Specialized;
    using  System.Data;
//    using  System.Data.SqlClient;
    using MySql.Data.MySqlClient;
    using  System.Data.SqlTypes;
    using  System.Security.Cryptography;
    using  System.Text;
    using  System.Text.RegularExpressions;
    using  System.Configuration.Provider;
    using  System.Configuration;
    using  System.Web.DataAccess;
    using  System.Web.Management;
    using  System.Web.Util;

    /// <devdoc>
    ///    <para>[To be supplied.]</para>
    /// </devdoc>
    // Remove CAS from sample: [AspNetHostingPermission(SecurityAction.LinkDemand, Level=AspNetHostingPermissionLevel.Minimal)]
    // Remove CAS from sample: [AspNetHostingPermission(SecurityAction.InheritanceDemand, Level=AspNetHostingPermissionLevel.Minimal)]
    public class MySqlMembershipProvider : MembershipProvider
    {
        ////////////////////////////////////////////////////////////
        // Public properties

        public override bool    EnablePasswordRetrieval   { get { return _EnablePasswordRetrieval; } }

        public override bool    EnablePasswordReset       { get { return _EnablePasswordReset; } }

        public override bool    RequiresQuestionAndAnswer   { get { return _RequiresQuestionAndAnswer; } }

        public override bool    RequiresUniqueEmail         { get { return _RequiresUniqueEmail; } }

        public override MembershipPasswordFormat PasswordFormat { get { return _PasswordFormat; }}
        public override int MaxInvalidPasswordAttempts { get { return _MaxInvalidPasswordAttempts; } }

        public override int PasswordAttemptWindow { get { return _PasswordAttemptWindow; } }

        public override int MinRequiredPasswordLength
        {
            get { return _MinRequiredPasswordLength; }
        }

        public override int MinRequiredNonAlphanumericCharacters
        {
            get { return _MinRequiredNonalphanumericCharacters; }
        }

        public override string PasswordStrengthRegularExpression
        {
            get { return _PasswordStrengthRegularExpression; }
        }

        public override string ApplicationName
        {
            get { return _AppName; }
            set
            {
                if (String.IsNullOrEmpty(value))
                    throw new ArgumentNullException("value");

                if (value.Length > 256)
                    throw new ProviderException( SR.GetString( SR.Provider_application_name_too_long ) );
                _AppName = value;
            }
        }

        private string    _sqlConnectionString;
        private bool      _EnablePasswordRetrieval;
        private bool      _EnablePasswordReset;
        private bool      _RequiresQuestionAndAnswer;
        private string    _AppName;
        private bool      _RequiresUniqueEmail;
        private int       _MaxInvalidPasswordAttempts;
        private int       _CommandTimeout;
        private int       _PasswordAttemptWindow;
        private int       _MinRequiredPasswordLength;
        private int       _MinRequiredNonalphanumericCharacters;
        private string    _PasswordStrengthRegularExpression;
        private int       _SchemaVersionCheck;
        private MembershipPasswordFormat _PasswordFormat;

        private const int      PASSWORD_SIZE  = 14;

        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////

        public override void Initialize(string name, NameValueCollection config)
        {
            // Remove CAS from sample: HttpRuntime.CheckAspNetHostingPermission (AspNetHostingPermissionLevel.Low, SR.Feature_not_supported_at_this_level);
            if (config == null)
                throw new ArgumentNullException("config");
            if (String.IsNullOrEmpty(name))
                name = "MySqlMembershipProvider";
            if (string.IsNullOrEmpty(config["description"])) {
                config.Remove("description");
                config.Add("description", SR.GetString(SR.MembershipSqlProvider_description));
            }
            base.Initialize(name, config);

            _SchemaVersionCheck = 0;

            _EnablePasswordRetrieval    = SecUtility.GetBooleanValue(config, "enablePasswordRetrieval", false);
            _EnablePasswordReset        = SecUtility.GetBooleanValue(config, "enablePasswordReset", true);
            _RequiresQuestionAndAnswer  = SecUtility.GetBooleanValue(config, "requiresQuestionAndAnswer", true);
            _RequiresUniqueEmail        = SecUtility.GetBooleanValue(config, "requiresUniqueEmail", true);
            _MaxInvalidPasswordAttempts = SecUtility.GetIntValue( config, "maxInvalidPasswordAttempts", 5, false, 0 );
            _PasswordAttemptWindow      = SecUtility.GetIntValue( config, "passwordAttemptWindow", 10, false, 0 );
            _MinRequiredPasswordLength  = SecUtility.GetIntValue( config, "minRequiredPasswordLength", 7, false, 128 );
            _MinRequiredNonalphanumericCharacters = SecUtility.GetIntValue( config, "minRequiredNonalphanumericCharacters", 1, true, 128 );

            _PasswordStrengthRegularExpression = config["passwordStrengthRegularExpression"];
            if( _PasswordStrengthRegularExpression != null )
            {
                _PasswordStrengthRegularExpression = _PasswordStrengthRegularExpression.Trim();
                if( _PasswordStrengthRegularExpression.Length != 0 )
                {
                    try
                    {
                        Regex regex = new Regex( _PasswordStrengthRegularExpression );
                    }
                    catch( ArgumentException e )
                    {
                        throw new ProviderException( e.Message, e );
                    }
                }
            }
            else
            {
                _PasswordStrengthRegularExpression = string.Empty;
            }
            if (_MinRequiredNonalphanumericCharacters > _MinRequiredPasswordLength)
                throw new HttpException(SR.GetString(SR.MinRequiredNonalphanumericCharacters_can_not_be_more_than_MinRequiredPasswordLength));

            _CommandTimeout = SecUtility.GetIntValue( config, "commandTimeout", 30, true, 0 );
            _AppName = config["applicationName"];
            if (string.IsNullOrEmpty(_AppName))
                _AppName = SecUtility.GetDefaultAppName();

            if( _AppName.Length > 256 )
            {
                throw new ProviderException(SR.GetString(SR.Provider_application_name_too_long));
            }

            string strTemp = config["passwordFormat"];
            if (strTemp == null)
                strTemp = "Hashed";

            switch(strTemp)
            {
            case "Clear":
                _PasswordFormat = MembershipPasswordFormat.Clear;
                break;
            case "Encrypted":
                _PasswordFormat = MembershipPasswordFormat.Encrypted;
                break;
            case "Hashed":
                _PasswordFormat = MembershipPasswordFormat.Hashed;
                break;
            default:
                throw new ProviderException(SR.GetString(SR.Provider_bad_password_format));
            }

            if (PasswordFormat == MembershipPasswordFormat.Hashed && EnablePasswordRetrieval)
                throw new ProviderException(SR.GetString(SR.Provider_can_not_retrieve_hashed_password));
            //if (_PasswordFormat == MembershipPasswordFormat.Encrypted && MachineKeySection.IsDecryptionKeyAutogenerated)
            //    throw new ProviderException(SR.GetString(SR.Can_not_use_encrypted_passwords_with_autogen_keys));

            string temp = config["connectionStringName"];
            if (temp == null || temp.Length < 1)
                throw new ProviderException(SR.GetString(SR.Connection_name_not_specified));
            _sqlConnectionString = SqlConnectionHelper.GetConnectionString(temp, true, true);
            if (_sqlConnectionString == null || _sqlConnectionString.Length < 1) {
                throw new ProviderException(SR.GetString(SR.Connection_string_not_found, temp));
            }

            config.Remove("connectionStringName");
            config.Remove("enablePasswordRetrieval");
            config.Remove("enablePasswordReset");
            config.Remove("requiresQuestionAndAnswer");
            config.Remove("applicationName");
            config.Remove("requiresUniqueEmail");
            config.Remove("maxInvalidPasswordAttempts");
            config.Remove("passwordAttemptWindow");
            config.Remove("commandTimeout");
            config.Remove("passwordFormat");
            config.Remove("name");
            config.Remove("minRequiredPasswordLength");
            config.Remove("minRequiredNonalphanumericCharacters");
            config.Remove("passwordStrengthRegularExpression");
            if (config.Count > 0) {
                string attribUnrecognized = config.GetKey(0);
                if (!String.IsNullOrEmpty(attribUnrecognized))
                    throw new ProviderException(SR.GetString(SR.Provider_unrecognized_attribute, attribUnrecognized));
            }
        }

        private void CheckSchemaVersion( MySqlConnection connection )
        {
            string[] features = { "Common", "Membership" };
            string   version  = "1";

            SecUtility.CheckSchemaVersion( this,
                                           connection,
                                           features,
                                           version,
                                           ref _SchemaVersionCheck );
        }

        private int CommandTimeout
        {
            get{ return _CommandTimeout; }
        }

        ////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////

        public override MembershipUser CreateUser( string username,
                                                   string password,
                                                   string email,
                                                   string passwordQuestion,
                                                   string passwordAnswer,
                                                   bool   isApproved,
                                                   object providerUserKey,
                                                   out    MembershipCreateStatus status )
        {
            if( !SecUtility.ValidateParameter(ref password, true, true, false, 128))
            {
                status = MembershipCreateStatus.InvalidPassword;
                return null;
            }

            string salt = GenerateSalt();
            string pass = EncodePassword(password, (int)_PasswordFormat, salt);
            if ( pass.Length > 128 )
            {
                status = MembershipCreateStatus.InvalidPassword;
                return null;
            }

            string encodedPasswordAnswer;
            if( passwordAnswer != null )
            {
                passwordAnswer = passwordAnswer.Trim();
            }

            if (!string.IsNullOrEmpty(passwordAnswer)) {
                if( passwordAnswer.Length > 128 )
                {
                    status = MembershipCreateStatus.InvalidAnswer;
                    return null;
                }
                encodedPasswordAnswer = EncodePassword(passwordAnswer.ToLower(CultureInfo.InvariantCulture), (int)_PasswordFormat, salt);
            }
            else
                encodedPasswordAnswer = passwordAnswer;
            if (!SecUtility.ValidateParameter(ref encodedPasswordAnswer, RequiresQuestionAndAnswer, true, false, 128))
            {
                status = MembershipCreateStatus.InvalidAnswer;
                return null;
            }

            if( !SecUtility.ValidateParameter( ref username,true, true, true, 256))
            {
                status = MembershipCreateStatus.InvalidUserName;
                return null;
            }

            if( !SecUtility.ValidateParameter( ref email,
                                               RequiresUniqueEmail,
                                               RequiresUniqueEmail,
                                               false,
                                               256 ) )
            {
                status = MembershipCreateStatus.InvalidEmail;
                return null;
            }

            if( !SecUtility.ValidateParameter( ref passwordQuestion, RequiresQuestionAndAnswer, true, false, 256))
            {
                status = MembershipCreateStatus.InvalidQuestion;
                return null;
            }

            if( providerUserKey != null )
            {
                if( !( providerUserKey is Guid ) )
                {
                    status = MembershipCreateStatus.InvalidProviderUserKey;
                    return null;
                }
            }

            if( password.Length < MinRequiredPasswordLength )
            {
                status = MembershipCreateStatus.InvalidPassword;
                return null;
            }

            int count = 0;

            for( int i = 0; i < password.Length; i++ )
            {
                if( !char.IsLetterOrDigit( password, i ) )
                {
                    count++;
                }
            }

            if( count < MinRequiredNonAlphanumericCharacters )
            {
                status = MembershipCreateStatus.InvalidPassword;
                return null;
            }

            if( PasswordStrengthRegularExpression.Length > 0 )
            {
                if( !Regex.IsMatch( password, PasswordStrengthRegularExpression ) )
                {
                    status = MembershipCreateStatus.InvalidPassword;
                    return null;
                }
            }

            ValidatePasswordEventArgs e = new ValidatePasswordEventArgs( username, password, true );
            OnValidatingPassword( e );

            if( e.Cancel )
            {
                status = MembershipCreateStatus.InvalidPassword;
                return null;
            }

            try
            {
                SqlConnectionHolder holder = null;
                try {
                    holder = SqlConnectionHelper.GetConnection( _sqlConnectionString, true );
                    CheckSchemaVersion( holder.Connection );

                    DateTime    dt  = RoundToSeconds(DateTime.UtcNow);



                    int iStatus = MySqlStoredProcedures.aspnet_Membership_CreateUser(ApplicationName, username, 
                           pass, salt, email, passwordQuestion, encodedPasswordAnswer, isApproved, 
                           dt, dt, RequiresUniqueEmail, (int)PasswordFormat, ref providerUserKey, holder);

                    //int iStatus = ((UserId!=null) ? ((int) p.Value) : -1);
                    if (iStatus < 0 || iStatus > (int) MembershipCreateStatus.ProviderError)
                        iStatus = (int) MembershipCreateStatus.ProviderError;
                    status = (MembershipCreateStatus) iStatus;
                    if (iStatus != 0) // !success
                        return null;

                    //providerUserKey = new Guid( cmd.Parameters[ "@UserId" ].Value.ToString() );
                    dt = dt.ToLocalTime();
                    return new MembershipUser( this.Name,
                                               username,
                                               providerUserKey,
                                               email,
                                               passwordQuestion,
                                               null,
                                               isApproved,
                                               false,
                                               dt,
                                               dt,
                                               dt,
                                               dt,
                                               new DateTime( 1754, 1, 1 ) );
                }
                finally
                {
                    if( holder != null )
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            } catch {
                throw;
            }
        }

        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////

        public override bool ChangePasswordQuestionAndAnswer(string username, string password, string newPasswordQuestion, string newPasswordAnswer)
        {
            SecUtility.CheckParameter( ref username, true, true, true, 256, "username" );
            SecUtility.CheckParameter( ref password, true, true, false, 128, "password" );

            string salt;
            int passwordFormat;
            if (!CheckPassword(username, password, false, false, out salt, out passwordFormat))
                return false;
            SecUtility.CheckParameter(ref newPasswordQuestion, RequiresQuestionAndAnswer, RequiresQuestionAndAnswer, false, 256, "newPasswordQuestion");
            string encodedPasswordAnswer;
            if( newPasswordAnswer != null )
            {
                newPasswordAnswer = newPasswordAnswer.Trim();
            }

            SecUtility.CheckParameter(ref newPasswordAnswer, RequiresQuestionAndAnswer, RequiresQuestionAndAnswer, false, 128, "newPasswordAnswer");
            if (!string.IsNullOrEmpty(newPasswordAnswer)) {
                encodedPasswordAnswer = EncodePassword(newPasswordAnswer.ToLower(CultureInfo.InvariantCulture), (int)passwordFormat, salt);
            }
            else
                encodedPasswordAnswer = newPasswordAnswer;
            SecUtility.CheckParameter(ref encodedPasswordAnswer, RequiresQuestionAndAnswer, RequiresQuestionAndAnswer, false, 128, "newPasswordAnswer");

            try {
                SqlConnectionHolder holder = null;
                try {
                    holder = SqlConnectionHelper.GetConnection( _sqlConnectionString, true );
                    CheckSchemaVersion( holder.Connection );

                    //MySqlCommand    cmd     = new MySqlCommand("dbo.aspnet_Membership_ChangePasswordQuestionAndAnswer", holder.Connection);

                    //cmd.CommandTimeout = CommandTimeout;
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add(CreateInputParam("@ApplicationName", SqlDbType.NVarChar, ApplicationName));
                    //cmd.Parameters.Add(CreateInputParam("@UserName", SqlDbType.NVarChar, username));
                    //cmd.Parameters.Add(CreateInputParam("@NewPasswordQuestion", SqlDbType.NVarChar, newPasswordQuestion));
                    //cmd.Parameters.Add(CreateInputParam("@NewPasswordAnswer", SqlDbType.NVarChar, encodedPasswordAnswer));

                    //MySqlParameter p = new MySqlParameter("@ReturnValue", SqlDbType.Int);
                    //p.Direction = ParameterDirection.ReturnValue;
                    //cmd.Parameters.Add(p);

                    //cmd.ExecuteNonQuery();

                    int status = MySqlStoredProcedures.aspnet_Membership_ChangePasswordQuestionAndAnswer(ApplicationName,
                            username, newPasswordQuestion, encodedPasswordAnswer, holder);

                    if( status != 0 )
                    {
                        throw new ProviderException( GetExceptionText( status ) );
                    }

                    return ( status == 0 );
                }
                finally
                {
                    if( holder != null )
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            } catch {
                throw;
            }
        }

        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////

        public override string GetPassword(string username, string passwordAnswer)
        {
            if ( !EnablePasswordRetrieval )
            {
                throw new NotSupportedException( SR.GetString( SR.Membership_PasswordRetrieval_not_supported ) );
            }

            SecUtility.CheckParameter( ref username, true, true, true, 256, "username" );

            string encodedPasswordAnswer = GetEncodedPasswordAnswer(username, passwordAnswer);
            SecUtility.CheckParameter(ref encodedPasswordAnswer, RequiresQuestionAndAnswer, RequiresQuestionAndAnswer, false, 128, "passwordAnswer");

            string errText;
            int passwordFormat = 0;
            int status = 0;

            string pass = GetPasswordFromDB(username, encodedPasswordAnswer, RequiresQuestionAndAnswer, out passwordFormat, out status);

            if ( pass == null )
            {
                errText = GetExceptionText( status );
                if ( IsStatusDueToBadPassword( status ) )
                {
                    throw new MembershipPasswordException( errText );
                }
                else
                {
                    throw new ProviderException( errText );
                }
            }

            return UnEncodePassword( pass, passwordFormat );
        }

        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////

        public override bool ChangePassword(string username, string oldPassword, string newPassword)
        {
            SecUtility.CheckParameter( ref username, true, true, true, 256, "username" );
            SecUtility.CheckParameter( ref oldPassword, true, true, false, 128, "oldPassword" );
            SecUtility.CheckParameter( ref newPassword, true, true, false, 128, "newPassword" );

            string salt = null;
            int passwordFormat;
            int status;

            if (!CheckPassword( username, oldPassword, false, false, out salt, out passwordFormat))
            {
               return false;
            }

            if( newPassword.Length < MinRequiredPasswordLength )
            {
                throw new ArgumentException(SR.GetString(
                              SR.Password_too_short,
                              "newPassword",
                              MinRequiredPasswordLength.ToString(CultureInfo.InvariantCulture)));
            }

            int count = 0;

            for( int i = 0; i < newPassword.Length; i++ )
            {
                if( !char.IsLetterOrDigit( newPassword, i ) )
                {
                    count++;
                }
            }

            if( count < MinRequiredNonAlphanumericCharacters )
            {
                throw new ArgumentException(SR.GetString(
                              SR.Password_need_more_non_alpha_numeric_chars,
                              "newPassword",
                              MinRequiredNonAlphanumericCharacters.ToString(CultureInfo.InvariantCulture)));
            }

            if( PasswordStrengthRegularExpression.Length > 0 )
            {
                if( !Regex.IsMatch( newPassword, PasswordStrengthRegularExpression ) )
                {
                    throw new ArgumentException(SR.GetString(SR.Password_does_not_match_regular_expression,
                                                             "newPassword"));
                }
            }

            string pass = EncodePassword(newPassword, (int)passwordFormat, salt);
            if ( pass.Length > 128 )
            {
                throw new ArgumentException(SR.GetString(SR.Membership_password_too_long), "newPassword");
            }

            ValidatePasswordEventArgs e = new ValidatePasswordEventArgs( username, newPassword, false );
            OnValidatingPassword( e );

            if( e.Cancel )
            {
                if( e.FailureInformation != null )
                {
                    throw e.FailureInformation;
                }
                else
                {
                    throw new ArgumentException( SR.GetString( SR.Membership_Custom_Password_Validation_Failure ), "newPassword");
                }
            }


            try {
                SqlConnectionHolder holder = null;
                try {
                    holder = SqlConnectionHelper.GetConnection( _sqlConnectionString, true );
                    CheckSchemaVersion( holder.Connection );

                    //MySqlCommand  cmd = new MySqlCommand( "dbo.aspnet_Membership_SetPassword", holder.Connection );

                    //cmd.CommandTimeout = CommandTimeout;
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add(CreateInputParam("@ApplicationName", SqlDbType.NVarChar, ApplicationName));
                    //cmd.Parameters.Add(CreateInputParam("@UserName", SqlDbType.NVarChar, username));
                    //cmd.Parameters.Add(CreateInputParam("@NewPassword", SqlDbType.NVarChar, pass));
                    //cmd.Parameters.Add(CreateInputParam("@PasswordSalt", SqlDbType.NVarChar, salt));
                    //cmd.Parameters.Add(CreateInputParam("@PasswordFormat", SqlDbType.Int, passwordFormat));
                    //cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", SqlDbType.DateTime, DateTime.UtcNow));

                    //MySqlParameter p = new MySqlParameter("@ReturnValue", SqlDbType.Int);
                    //p.Direction = ParameterDirection.ReturnValue;
                    //cmd.Parameters.Add(p);

                    //cmd.ExecuteNonQuery();

                    //status =  ( ( p.Value != null ) ? ( ( int )p.Value ) : -1 );

                    status = MySqlStoredProcedures.aspnet_Membership_SetPassword(ApplicationName, username, pass,
                            salt, DateTime.UtcNow, passwordFormat, holder);

                    if ( status != 0 )
                    {
                        string errText = GetExceptionText( status );

                        if ( IsStatusDueToBadPassword( status ) )
                        {
                            throw new MembershipPasswordException( errText );
                        }
                        else
                        {
                            throw new ProviderException( errText );
                        }
                    }

                    return true;
                }
                finally
                {
                    if( holder != null )
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            } catch {
                throw;
            }
        }

        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////

        public override string ResetPassword( string username, string passwordAnswer )
        {
            if ( !EnablePasswordReset )
            {
                throw new NotSupportedException( SR.GetString( SR.Not_configured_to_support_password_resets ) );
            }

            SecUtility.CheckParameter( ref username, true, true, true, 256, "username" );

            string salt;
            int passwordFormat;
            string passwdFromDB;
            int status;
            int failedPasswordAttemptCount;
            int failedPasswordAnswerAttemptCount;
            bool isApproved;
            DateTime lastLoginDate, lastActivityDate;

            GetPasswordWithFormat(username, false, out status, out passwdFromDB, out passwordFormat, out salt, out failedPasswordAttemptCount,
                                  out failedPasswordAnswerAttemptCount, out isApproved, out lastLoginDate, out lastActivityDate);
            if (status != 0)
            {
                if (IsStatusDueToBadPassword(status))
                {
                    throw new MembershipPasswordException(GetExceptionText(status));
                }
                else
                {
                    throw new ProviderException(GetExceptionText(status));
                }
            }

            string encodedPasswordAnswer;
            if( passwordAnswer != null )
            {
                passwordAnswer = passwordAnswer.Trim();
            }
            if (!string.IsNullOrEmpty(passwordAnswer))
                encodedPasswordAnswer = EncodePassword(passwordAnswer.ToLower(CultureInfo.InvariantCulture), passwordFormat, salt);
            else
                encodedPasswordAnswer = passwordAnswer;
            SecUtility.CheckParameter(ref encodedPasswordAnswer, RequiresQuestionAndAnswer, RequiresQuestionAndAnswer, false, 128, "passwordAnswer");
            string newPassword  = GeneratePassword();

            ValidatePasswordEventArgs e = new ValidatePasswordEventArgs( username, newPassword, false );
            OnValidatingPassword( e );

            if( e.Cancel )
            {
                if( e.FailureInformation != null )
                {
                    throw e.FailureInformation;
                }
                else
                {
                    throw new ProviderException( SR.GetString( SR.Membership_Custom_Password_Validation_Failure ) );
                }
            }


            try
            {
                SqlConnectionHolder holder = null;
                try {
                    holder = SqlConnectionHelper.GetConnection( _sqlConnectionString, true );
                    CheckSchemaVersion( holder.Connection );

                    //MySqlCommand    cmd     = new MySqlCommand("dbo.aspnet_Membership_ResetPassword", holder.Connection);
                    //string        errText;

                    //cmd.CommandTimeout = CommandTimeout;
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add(CreateInputParam("@ApplicationName", SqlDbType.NVarChar, ApplicationName));
                    //cmd.Parameters.Add(CreateInputParam("@UserName", SqlDbType.NVarChar, username));
                    //cmd.Parameters.Add(CreateInputParam("@NewPassword", SqlDbType.NVarChar, EncodePassword(newPassword, (int) passwordFormat, salt)));
                    //cmd.Parameters.Add(CreateInputParam("@MaxInvalidPasswordAttempts", SqlDbType.Int, MaxInvalidPasswordAttempts ) );
                    //cmd.Parameters.Add(CreateInputParam("@PasswordAttemptWindow", SqlDbType.Int, PasswordAttemptWindow ) );
                    //cmd.Parameters.Add(CreateInputParam("@PasswordSalt", SqlDbType.NVarChar, salt));
                    //cmd.Parameters.Add(CreateInputParam("@PasswordFormat", SqlDbType.Int, (int)passwordFormat));
                    //cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", SqlDbType.DateTime, DateTime.UtcNow));
                    //if (RequiresQuestionAndAnswer) {
                    //    cmd.Parameters.Add(CreateInputParam("@PasswordAnswer", SqlDbType.NVarChar, encodedPasswordAnswer));
                    //}

                    //MySqlParameter p = new MySqlParameter("@ReturnValue", SqlDbType.Int);
                    //p.Direction = ParameterDirection.ReturnValue;
                    //cmd.Parameters.Add(p);

                    //cmd.ExecuteNonQuery();

                    status = MySqlStoredProcedures.aspnet_Membership_ResetPassword(ApplicationName, username,
                        EncodePassword(newPassword,(int) passwordFormat, salt), MaxInvalidPasswordAttempts, PasswordAttemptWindow,
                        salt, DateTime.UtcNow, (int)passwordFormat, encodedPasswordAnswer, holder);

                    if ( status != 0 )
                    {
                        String errText = GetExceptionText( status );

                        if ( IsStatusDueToBadPassword( status ) )
                        {
                            throw new MembershipPasswordException( errText );
                        }
                        else
                        {
                            throw new ProviderException( errText );
                        }
                    }

                    return newPassword;
                }
                finally
                {
                    if( holder != null )
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            } catch {
                throw;
            }
        }

        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////

        public override void UpdateUser(MembershipUser user)
        {
            if( user == null )
            {
                throw new ArgumentNullException( "user" );
            }

            string temp = user.UserName;
            SecUtility.CheckParameter( ref temp, true, true, true, 256, "UserName" );
            temp = user.Email;
            SecUtility.CheckParameter( ref temp,
                                       RequiresUniqueEmail,
                                       RequiresUniqueEmail,
                                       false,
                                       256,
                                       "Email");
            user.Email = temp;
            try {
                SqlConnectionHolder holder = null;
                try {
                    holder = SqlConnectionHelper.GetConnection( _sqlConnectionString, true );
                    CheckSchemaVersion( holder.Connection );

                    //MySqlCommand    cmd     = new MySqlCommand("dbo.aspnet_Membership_UpdateUser", holder.Connection);

                    //cmd.CommandTimeout = CommandTimeout;
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add(CreateInputParam("@ApplicationName", SqlDbType.NVarChar, ApplicationName));
                    //cmd.Parameters.Add(CreateInputParam("@UserName", SqlDbType.NVarChar, user.UserName));
                    //cmd.Parameters.Add(CreateInputParam("@Email", SqlDbType.NVarChar, user.Email));
                    //cmd.Parameters.Add(CreateInputParam("@Comment", SqlDbType.NText, user.Comment));
                    //cmd.Parameters.Add(CreateInputParam("@IsApproved", SqlDbType.Bit, user.IsApproved ? 1 : 0));
                    //cmd.Parameters.Add(CreateInputParam("@LastLoginDate", SqlDbType.DateTime, user.LastLoginDate.ToUniversalTime()));
                    //cmd.Parameters.Add(CreateInputParam("@LastActivityDate", SqlDbType.DateTime, user.LastActivityDate.ToUniversalTime()));
                    //cmd.Parameters.Add(CreateInputParam("@UniqueEmail", SqlDbType.Int, RequiresUniqueEmail ? 1 : 0));
                    //cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", SqlDbType.DateTime, DateTime.UtcNow));

                    //MySqlParameter p = new MySqlParameter("@ReturnValue", SqlDbType.Int);
                    //p.Direction = ParameterDirection.ReturnValue;
                    //cmd.Parameters.Add(p);
                    //cmd.ExecuteNonQuery();

                    int status = MySqlStoredProcedures.aspnet_Membership_UpdateUser(ApplicationName, user.UserName,
                        user.Email, user.Comment, user.IsApproved, user.LastLoginDate.ToUniversalTime(),
                        user.LastActivityDate.ToUniversalTime(), RequiresUniqueEmail, DateTime.UtcNow, holder);
                    
                    //int status = ((p.Value!=null) ? ((int) p.Value) : -1);
                    if (status != 0)
                        throw new ProviderException(GetExceptionText(status));
                    return;
                }
                finally
                {
                    if( holder != null )
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            } catch {
                throw;
            }
        }

        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////

        public override bool ValidateUser(string username, string password)
        {
            if (    SecUtility.ValidateParameter(ref username, true, true, true, 256) &&
                    SecUtility.ValidateParameter(ref password, true, true, false, 128) &&
                    CheckPassword(username, password, true, true))
            {
                // Comment out perf counters in sample: PerfCounters.IncrementCounter(AppPerfCounter.MEMBER_SUCCESS);
                // Comment out events in sample: WebBaseEvent.RaiseSystemEvent(null, WebEventCodes.AuditMembershipAuthenticationSuccess, username);
                return true;
            } else {
                // Comment out perf counters in sample: PerfCounters.IncrementCounter(AppPerfCounter.MEMBER_FAIL);
                // Comment out events in sample: WebBaseEvent.RaiseSystemEvent(null, WebEventCodes.AuditMembershipAuthenticationFailure, username);
                return false;
            }
        }

        public override bool UnlockUser( string username )
        {
            SecUtility.CheckParameter(ref username, true, true, true, 256, "username" );
            try {
                SqlConnectionHolder holder = null;
                try {
                    holder = SqlConnectionHelper.GetConnection( _sqlConnectionString, true );
                    CheckSchemaVersion(holder.Connection);

                    //MySqlCommand cmd = new MySqlCommand("dbo.aspnet_Membership_UnlockUser", holder.Connection);

                    //cmd.CommandTimeout = CommandTimeout;
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add(CreateInputParam("@ApplicationName", SqlDbType.NVarChar, ApplicationName));
                    //cmd.Parameters.Add(CreateInputParam("@UserName", SqlDbType.NVarChar, username));

                    //MySqlParameter p = new MySqlParameter("@ReturnValue", SqlDbType.Int);
                    //p.Direction = ParameterDirection.ReturnValue;
                    //cmd.Parameters.Add(p);

                    //cmd.ExecuteNonQuery();

                    int status = MySqlStoredProcedures.aspnet_Membership_UnlockUser(ApplicationName,
                            username, holder);

                    //int status = ((p.Value != null) ? ((int)p.Value) : -1);
                    if (status == 0) {
                        return true;
                    }

                    return false;
                }
                finally {
                    if( holder != null )
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            } catch {
                throw;
            }
        }

        public override MembershipUser GetUser(object providerUserKey, bool userIsOnline)
        {
            if (providerUserKey == null)
            {
                throw new ArgumentNullException("providerUserKey");
            }

            if (!(providerUserKey is Guid))
            {
                throw new ArgumentException(SR.GetString(SR.Membership_InvalidProviderUserKey), "providerUserKey");
            }

            MySqlDataReader reader = null;

            try
            {
                SqlConnectionHolder holder = null;
                try
                {
                    holder = SqlConnectionHelper.GetConnection(_sqlConnectionString, true);
                    CheckSchemaVersion(holder.Connection);

                    MySqlCommand cmd = new MySqlCommand(); //"dbo.aspnet_Membership_GetUserByUserId", holder.Connection);

                    int status = MySqlStoredProcedures.aspnet_Membership_GetUserByUserId((Guid)providerUserKey, DateTime.UtcNow,
                            userIsOnline, holder, ref cmd);

                    cmd.CommandTimeout = CommandTimeout;
                    
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add(CreateInputParam("@UserId", SqlDbType.UniqueIdentifier, providerUserKey));
                    //cmd.Parameters.Add(CreateInputParam("@UpdateLastActivity", SqlDbType.Bit, userIsOnline));
                    //cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", SqlDbType.DateTime, DateTime.UtcNow));
                    //SqlParameter p = new SqlParameter("@ReturnValue", SqlDbType.Int);
                    //p.Direction = ParameterDirection.ReturnValue;
                    //cmd.Parameters.Add(p);

                    reader = cmd.ExecuteReader();
                    if (reader.Read())
                    {
                        string email = GetNullableString(reader, 0);
                        string passwordQuestion = GetNullableString(reader, 1);
                        string comment = GetNullableString(reader, 2);
                        bool isApproved = reader.GetBoolean(3);
                        DateTime dtCreate = reader.GetDateTime(4).ToLocalTime();
                        DateTime dtLastLogin = reader.GetDateTime(5).ToLocalTime();
                        DateTime dtLastActivity = reader.GetDateTime(6).ToLocalTime();
                        DateTime dtLastPassChange = reader.GetDateTime(7).ToLocalTime();
                        string userName = GetNullableString(reader, 8);
                        bool isLockedOut = reader.GetBoolean(9);
                        DateTime dtLastLockoutDate = reader.GetDateTime(10).ToLocalTime();

                        ////////////////////////////////////////////////////////////
                        // Step 4 : Return the result
                        return new MembershipUser(this.Name,
                                                   userName,
                                                   providerUserKey,
                                                   email,
                                                   passwordQuestion,
                                                   comment,
                                                   isApproved,
                                                   isLockedOut,
                                                   dtCreate,
                                                   dtLastLogin,
                                                   dtLastActivity,
                                                   dtLastPassChange,
                                                   dtLastLockoutDate);
                    }

                    return null;
                }
                finally
                {
                    if (reader != null)
                    {
                        reader.Close();
                        reader = null;
                    }

                    if (holder != null)
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////

        public override MembershipUser GetUser(string username, bool userIsOnline)
        {
            SecUtility.CheckParameter(
                            ref username,
                            true,
                            false,
                            true,
                            256,
                            "username");

            MySqlDataReader reader = null;

            try
            {
                SqlConnectionHolder holder = null;
                try
                {
                    holder = SqlConnectionHelper.GetConnection(_sqlConnectionString, true);
                    CheckSchemaVersion(holder.Connection);

                    MySqlCommand cmd = new MySqlCommand(); //"dbo.aspnet_Membership_GetUserByName", holder.Connection);

                    int status = MySqlStoredProcedures.aspnet_Membership_GetUserByName(ApplicationName, username,
                            DateTime.UtcNow, userIsOnline, holder, ref cmd);

                    cmd.CommandTimeout = CommandTimeout;
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add(CreateInputParam("@ApplicationName", SqlDbType.NVarChar, ApplicationName));
                    //cmd.Parameters.Add(CreateInputParam("@UserName", SqlDbType.NVarChar, username));
                    //cmd.Parameters.Add(CreateInputParam("@UpdateLastActivity", SqlDbType.Bit, userIsOnline));
                    //cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", SqlDbType.DateTime, DateTime.UtcNow));
                    //SqlParameter p = new SqlParameter("@ReturnValue", SqlDbType.Int);
                    //p.Direction = ParameterDirection.ReturnValue;
                    //cmd.Parameters.Add(p);

                    reader = cmd.ExecuteReader();
                    if (reader.Read())
                    {
                        string email = GetNullableString(reader, 0);
                        string passwordQuestion = GetNullableString(reader, 1);
                        string comment = GetNullableString(reader, 2);
                        bool isApproved = reader.GetBoolean(3);
                        DateTime dtCreate = reader.GetDateTime(4).ToLocalTime();
                        DateTime dtLastLogin = reader.GetDateTime(5).ToLocalTime();
                        DateTime dtLastActivity = reader.GetDateTime(6).ToLocalTime();
                        DateTime dtLastPassChange = reader.GetDateTime(7).ToLocalTime();
                        Guid userId = reader.GetGuid(8);
                        bool isLockedOut = reader.GetBoolean(9);
                        DateTime dtLastLockoutDate = reader.GetDateTime(10).ToLocalTime();

                        ////////////////////////////////////////////////////////////
                        // Step 4 : Return the result
                        return new MembershipUser(this.Name,
                                                   username,
                                                   userId,
                                                   email,
                                                   passwordQuestion,
                                                   comment,
                                                   isApproved,
                                                   isLockedOut,
                                                   dtCreate,
                                                   dtLastLogin,
                                                   dtLastActivity,
                                                   dtLastPassChange,
                                                   dtLastLockoutDate);
                    }

                    return null;

                }
                finally
                {
                    if (reader != null)
                    {
                        reader.Close();
                        reader = null;
                    }

                    if (holder != null)
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////

        public override string GetUserNameByEmail(string email)
        {
            SecUtility.CheckParameter(
                            ref email,
                            false,
                            false,
                            false,
                            256,
                            "email");


            try
            {
                SqlConnectionHolder holder = null;
                try
                {
                    holder = SqlConnectionHelper.GetConnection(_sqlConnectionString, true);
                    CheckSchemaVersion(holder.Connection);

                    MySqlCommand cmd = new MySqlCommand(); //"dbo.aspnet_Membership_GetUserByEmail", holder.Connection);
                    string username = null;
                    MySqlDataReader reader = null;

                    int status = MySqlStoredProcedures.aspnet_Membership_GetUserByEmail(ApplicationName, email,
                        holder, ref cmd);

                    cmd.CommandTimeout = CommandTimeout;
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add(CreateInputParam("@ApplicationName", SqlDbType.NVarChar, ApplicationName));
                    //cmd.Parameters.Add(CreateInputParam("@Email", SqlDbType.NVarChar, email));

                    //SqlParameter p = new SqlParameter("@ReturnValue", SqlDbType.Int);
                    //p.Direction = ParameterDirection.ReturnValue;
                    //cmd.Parameters.Add(p);
                    try
                    {
                        reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess);
                        if (reader.Read())
                        {
                            username = GetNullableString(reader, 0);
                            if (RequiresUniqueEmail && reader.Read())
                            {
                                throw new ProviderException(SR.GetString(SR.Membership_more_than_one_user_with_email));
                            }
                        }
                    }
                    finally
                    {
                        if (reader != null)
                            reader.Close();
                    }
                    return username;
                }
                finally
                {
                    if (holder != null)
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            }
            catch
            {
                throw;
            }
        }

        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////

        public override bool DeleteUser(string username, bool deleteAllRelatedData)
        {
            SecUtility.CheckParameter( ref username, true, true, true, 256, "username" );

            try {
                SqlConnectionHolder holder = null;
                try {
                    holder = SqlConnectionHelper.GetConnection(_sqlConnectionString, true);
                    CheckSchemaVersion( holder.Connection );

                    int TablesToDeleteFrom = 1;

                    if (deleteAllRelatedData)
                    {
                        TablesToDeleteFrom = 16;
                    }

                    int NumTableDeletedFrom = 0;

                    int status = MySqlStoredProcedures.aspnet_Users_DeleteUser(ApplicationName, username,
                            TablesToDeleteFrom, ref NumTableDeletedFrom, holder);


                    //MySqlCommand cmd = new MySqlCommand("dbo.aspnet_Users_DeleteUser", holder.Connection);

                    //cmd.CommandTimeout = CommandTimeout;
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add(CreateInputParam("@ApplicationName", SqlDbType.NVarChar, ApplicationName));
                    //cmd.Parameters.Add(CreateInputParam("@UserName", SqlDbType.NVarChar, username));

                    //if( deleteAllRelatedData )
                    //{
                    //    cmd.Parameters.Add(CreateInputParam("@TablesToDeleteFrom", SqlDbType.Int, 0xF));
                    //}
                    //else
                    //{
                    //    cmd.Parameters.Add(CreateInputParam("@TablesToDeleteFrom", SqlDbType.Int, 1));
                    //}

                    //MySqlParameter p = new MySqlParameter("@NumTablesDeletedFrom", SqlDbType.Int);
                    //p.Direction = ParameterDirection.Output;
                    //cmd.Parameters.Add(p);
                    //cmd.ExecuteNonQuery();

                    return (NumTableDeletedFrom > 0);
                }
                finally
                {
                    if( holder != null )
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            } catch {
                throw;
            }
        }


        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////


        public override MembershipUserCollection GetAllUsers(int pageIndex, int pageSize, out int totalRecords)
        {
            if ( pageIndex < 0 )
                throw new ArgumentException(SR.GetString(SR.PageIndex_bad), "pageIndex");
            if ( pageSize < 1 )
                throw new ArgumentException(SR.GetString(SR.PageSize_bad), "pageSize");

            long upperBound = (long)pageIndex * pageSize + pageSize - 1;
            if ( upperBound > Int32.MaxValue )
                throw new ArgumentException(SR.GetString(SR.PageIndex_PageSize_bad), "pageIndex and pageSize");

            MembershipUserCollection users   = new MembershipUserCollection();
            totalRecords = 0;
            try {
                SqlConnectionHolder holder = null;
                try {
                    holder = SqlConnectionHelper.GetConnection(_sqlConnectionString, true);
                    CheckSchemaVersion( holder.Connection );

                    int rowcount = MySqlStoredProcedures.aspnet_Membership_GetAllUsers(ApplicationName, holder);

                    int PageLowerBound = pageSize * pageIndex;
                    int PageUpperBound = pageSize - 1 + @PageLowerBound;

                    String sQuery = "SELECT aspnet_Users.UserName, aspnet_Membership.Email, " +
                        "aspnet_Membership.PasswordQuestion, aspnet_Membership.Comment, " +
                        "aspnet_Membership.IsApproved, aspnet_Membership.CreateDate, " +
                        "aspnet_Membership.LastLoginDate, aspnet_Users.LastActivityDate, " +
                        "aspnet_Membership.LastPasswordChangedDate, aspnet_Users.UserId, aspnet_Membership.IsLockedOut, " +
                        "aspnet_Membership.LastLockoutDate FROM   aspnet_Membership, aspnet_Users, " +
                        "PageIndexForUsers WHERE  aspnet_Users.UserId = PageIndexForUsers.UserId AND " +
                        "aspnet_Users.UserId = aspnet_Membership.UserId AND PageIndexForUsers.IndexId >= @PageLowerBound " +
                        "AND PageIndexForUsers.IndexId <= @PageUpperBound ORDER BY aspnet_Users.UserName";

                    MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);
                    MySqlDataReader            reader  = null;
                    
                    cmd.CommandTimeout = CommandTimeout;
                    cmd.Parameters.Add(CreateInputParam("@PageUpperBound", MySqlDbType.Int32, PageUpperBound));
                    cmd.Parameters.Add(CreateInputParam("@PageLowerBound", MySqlDbType.Int32, PageLowerBound));
                    
                    try {
                        reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess);
                        while (reader.Read()) {
                            string username, email, passwordQuestion, comment;
                            bool isApproved;
                            DateTime dtCreate, dtLastLogin, dtLastActivity, dtLastPassChange;
                            Guid userId;
                            bool isLockedOut;
                            DateTime dtLastLockoutDate;

                            username = GetNullableString( reader, 0 );
                            email = GetNullableString(reader, 1);
                            passwordQuestion = GetNullableString( reader, 2 );
                            comment = GetNullableString(reader, 3);
                            isApproved = reader.GetBoolean(4);
                            dtCreate = reader.GetDateTime(5).ToLocalTime();
                            dtLastLogin = reader.GetDateTime(6).ToLocalTime();
                            dtLastActivity = reader.GetDateTime(7).ToLocalTime();
                            dtLastPassChange = reader.GetDateTime(8).ToLocalTime();
                            userId = reader.GetGuid( 9 );
                            isLockedOut = reader.GetBoolean( 10 );
                            dtLastLockoutDate = reader.GetDateTime(11).ToLocalTime();

                            users.Add( new MembershipUser( this.Name,
                                                           username,
                                                           userId,
                                                           email,
                                                           passwordQuestion,
                                                           comment,
                                                           isApproved,
                                                           isLockedOut,
                                                           dtCreate,
                                                           dtLastLogin,
                                                           dtLastActivity,
                                                           dtLastPassChange,
                                                           dtLastLockoutDate ) );
                        }
                    }
                    finally
                    {
                        if (reader != null)
                            reader.Close();
                        totalRecords = rowcount;
                    }
                }
                finally
                {
                    if( holder != null )
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            } catch {
                throw;
            }
            return users;
        }
        /////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////

        public override int GetNumberOfUsersOnline()
        {

            try {
                SqlConnectionHolder holder = null;
                try {
                    holder = SqlConnectionHelper.GetConnection(_sqlConnectionString, true);
                    CheckSchemaVersion( holder.Connection );

                    DateTime DateActive = DateTime.UtcNow.AddMinutes(-Membership.UserIsOnlineTimeWindow);

                    String sQuery = "SELECT  @NumOnline = COUNT(*) " +
                        "FROM    aspnet_Users, " +
                        "aspnet_Applications, " +
                        "aspnet_Membership " +
                        "WHERE   aspnet_Users.ApplicationId = aspnet_Applications.ApplicationId AND " +
                        "LastActivityDate > @DateActive AND " +
                        "aspnet_Applications.LoweredApplicationName = @ApplicationName AND " +
                        "aspnet_Users.UserId = aspnet_Membership.UserId ";

                    MySqlCommand    cmd     = new MySqlCommand(sQuery, holder.Connection);

                    cmd.Parameters.Add(CreateInputParam("@DateActive", MySqlDbType.DateTime, DateActive));
                    cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

                    MySqlParameter p = new MySqlParameter("@NumOnline", MySqlDbType.Int16);
                    p.Direction = ParameterDirection.Output;
                    cmd.Parameters.Add(p);

                    cmd.ExecuteNonQuery();
                    
                    //MySqlCommand    cmd     = new MySqlCommand("dbo.aspnet_Membership_GetNumberOfUsersOnline", holder.Connection);
                    //MySqlParameter  p       = new MySqlParameter("@ReturnValue", SqlDbType.Int);

                    //cmd.CommandTimeout = CommandTimeout;
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add(CreateInputParam("@ApplicationName", SqlDbType.NVarChar, ApplicationName));
                    //cmd.Parameters.Add(CreateInputParam("@MinutesSinceLastInActive", SqlDbType.Int, Membership.UserIsOnlineTimeWindow));
                    //cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", SqlDbType.DateTime, DateTime.UtcNow));
                    //p.Direction = ParameterDirection.ReturnValue;
                    //cmd.Parameters.Add(p);
                    //cmd.ExecuteNonQuery();
                    int num = ((p.Value!=null) ? ((int) p.Value) : -1);
                    return num;
                }
                finally
                {
                    if( holder != null )
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            } catch {
                throw;
            }
        }

        /////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////

        public override MembershipUserCollection FindUsersByName(string usernameToMatch, int pageIndex, int pageSize, out int totalRecords)
        {
            SecUtility.CheckParameter(ref usernameToMatch, true, true, false, 256, "usernameToMatch");

            if ( pageIndex < 0 )
                throw new ArgumentException(SR.GetString(SR.PageIndex_bad), "pageIndex");
            if ( pageSize < 1 )
                throw new ArgumentException(SR.GetString(SR.PageSize_bad), "pageSize");

            long upperBound = (long)pageIndex * pageSize + pageSize - 1;
            if ( upperBound > Int32.MaxValue )
                throw new ArgumentException(SR.GetString(SR.PageIndex_PageSize_bad), "pageIndex and pageSize");

            try
            {
                SqlConnectionHolder holder = null;
                totalRecords = 0;
                MySqlParameter p = new MySqlParameter("@ReturnValue", SqlDbType.Int);
                p.Direction = ParameterDirection.ReturnValue;
                try {
                    holder = SqlConnectionHelper.GetConnection(_sqlConnectionString, true);
                    CheckSchemaVersion( holder.Connection );

                    int rowcount = MySqlStoredProcedures.aspnet_Membership_FindUsersByName(ApplicationName, 
                            usernameToMatch, holder);

                    int PageLowerBound = pageSize * pageIndex;
                    int PageUpperBound = pageSize - 1 + @PageLowerBound;

                    String sQuery = "SELECT aspnet_Users.UserName, aspnet_Membership.Email, " +
                        "aspnet_Membership.PasswordQuestion, aspnet_Membership.Comment, " +
                        "aspnet_Membership.IsApproved, aspnet_Membership.CreateDate, " +
                        "aspnet_Membership.LastLoginDate, aspnet_Users.LastActivityDate, " +
                        "aspnet_Membership.LastPasswordChangedDate, aspnet_Users.UserId, aspnet_Membership.IsLockedOut, " +
                        "aspnet_Membership.LastLockoutDate FROM   aspnet_Membership, aspnet_Users, " +
                        "PageIndexForUsers WHERE  aspnet_Users.UserId = PageIndexForUsers.UserId AND " +
                        "aspnet_Users.UserId = aspnet_Membership.UserId AND PageIndexForUsers.IndexId >= @PageLowerBound " +
                        "AND PageIndexForUsers.IndexId <= @PageUpperBound ORDER BY aspnet_Users.UserName";

                    MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);
                    MySqlDataReader reader = null;

                    cmd.CommandTimeout = CommandTimeout;
                    cmd.Parameters.Add(CreateInputParam("@PageUpperBound", MySqlDbType.Int16, PageUpperBound));
                    cmd.Parameters.Add(CreateInputParam("@PageLowerBound", MySqlDbType.Int16, PageLowerBound));
                    
                    //MySqlCommand cmd = new MySqlCommand("dbo.aspnet_Membership_FindUsersByName", holder.Connection);
                    MembershipUserCollection users = new MembershipUserCollection();
                    //MySqlDataReader reader = null;

                    //cmd.CommandTimeout = CommandTimeout;
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add(CreateInputParam("@ApplicationName", SqlDbType.NVarChar, ApplicationName));
                    //cmd.Parameters.Add(CreateInputParam("@UserNameToMatch", SqlDbType.NVarChar, usernameToMatch));
                    //cmd.Parameters.Add(CreateInputParam("@PageIndex", SqlDbType.Int, pageIndex));
                    //cmd.Parameters.Add(CreateInputParam("@PageSize", SqlDbType.Int, pageSize));
                    //cmd.Parameters.Add(p);
                    try
                    {
                        reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess);
                        while (reader.Read())
                        {
                            string username, email, passwordQuestion, comment;
                            bool isApproved;
                            DateTime dtCreate, dtLastLogin, dtLastActivity, dtLastPassChange;
                            Guid userId;
                            bool isLockedOut;
                            DateTime dtLastLockoutDate;

                            username = GetNullableString( reader, 0 );
                            email = GetNullableString(reader, 1);
                            passwordQuestion = GetNullableString( reader, 2 );
                            comment = GetNullableString(reader, 3);
                            isApproved = reader.GetBoolean(4);
                            dtCreate = reader.GetDateTime(5).ToLocalTime();
                            dtLastLogin = reader.GetDateTime(6).ToLocalTime();
                            dtLastActivity = reader.GetDateTime(7).ToLocalTime();
                            dtLastPassChange = reader.GetDateTime(8).ToLocalTime();
                            userId = reader.GetGuid( 9 );
                            isLockedOut = reader.GetBoolean( 10 );
                            dtLastLockoutDate = reader.GetDateTime(11).ToLocalTime();

                            users.Add( new MembershipUser( this.Name,
                                                           username,
                                                           userId,
                                                           email,
                                                           passwordQuestion,
                                                           comment,
                                                           isApproved,
                                                           isLockedOut,
                                                           dtCreate,
                                                           dtLastLogin,
                                                           dtLastActivity,
                                                           dtLastPassChange,
                                                           dtLastLockoutDate ) );
                        }

                        return users;
                    }
                    finally
                    {
                        if (reader != null)
                            reader.Close();
                        totalRecords = rowcount;
                    }
                }
                finally
                {
                    if( holder != null )
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            } catch {
                throw;
            }
        }
        /////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////
        public override MembershipUserCollection FindUsersByEmail(string emailToMatch, int pageIndex, int pageSize, out int totalRecords)
        {
            SecUtility.CheckParameter(ref emailToMatch, false, false, false, 256, "emailToMatch");

            if ( pageIndex < 0 )
                throw new ArgumentException(SR.GetString(SR.PageIndex_bad), "pageIndex");
            if ( pageSize < 1 )
                throw new ArgumentException(SR.GetString(SR.PageSize_bad), "pageSize");

            long upperBound = (long)pageIndex * pageSize + pageSize - 1;
            if ( upperBound > Int32.MaxValue )
                throw new ArgumentException(SR.GetString(SR.PageIndex_PageSize_bad), "pageIndex and pageSize");

            try {
                SqlConnectionHolder holder = null;
                totalRecords = 0;
                MySqlParameter p = new MySqlParameter("@ReturnValue", SqlDbType.Int);
                p.Direction = ParameterDirection.ReturnValue;
                try {
                    holder = SqlConnectionHelper.GetConnection(_sqlConnectionString, true);
                    CheckSchemaVersion( holder.Connection );

                    int rowcount = MySqlStoredProcedures.aspnet_Membership_FindUsersByEmail(ApplicationName,
                            emailToMatch, holder);

                    int PageLowerBound = pageSize * pageIndex;
                    int PageUpperBound = pageSize - 1 + @PageLowerBound;

                    String sQuery = "SELECT aspnet_Users.UserName, aspnet_Membership.Email, " +
                        "aspnet_Membership.PasswordQuestion, aspnet_Membership.Comment, " +
                        "aspnet_Membership.IsApproved, aspnet_Membership.CreateDate, " +
                        "aspnet_Membership.LastLoginDate, aspnet_Users.LastActivityDate, " +
                        "aspnet_Membership.LastPasswordChangedDate, aspnet_Users.UserId, aspnet_Membership.IsLockedOut, " +
                        "aspnet_Membership.LastLockoutDate FROM   aspnet_Membership, aspnet_Users, " +
                        "PageIndexForUsers WHERE  aspnet_Users.UserId = PageIndexForUsers.UserId AND " +
                        "aspnet_Users.UserId = aspnet_Membership.UserId AND PageIndexForUsers.IndexId >= @PageLowerBound " +
                        "AND PageIndexForUsers.IndexId <= @PageUpperBound ORDER BY aspnet_Users.UserName";

                    MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);
                    MySqlDataReader reader = null;

                    cmd.CommandTimeout = CommandTimeout;
                    cmd.Parameters.Add(CreateInputParam("@PageUpperBound", MySqlDbType.Int16, PageUpperBound));
                    cmd.Parameters.Add(CreateInputParam("@PageLowerBound", MySqlDbType.Int16, PageLowerBound));

                    //MySqlCommand cmd = new MySqlCommand("dbo.aspnet_Membership_FindUsersByName", holder.Connection);
                    MembershipUserCollection users = new MembershipUserCollection();
                    //MySqlDataReader reader = null;

                    //MySqlCommand cmd = new MySqlCommand("dbo.aspnet_Membership_FindUsersByEmail", holder.Connection);
                    //MembershipUserCollection users = new MembershipUserCollection();
                    //MySqlDataReader reader = null;

                    //cmd.CommandTimeout = CommandTimeout;
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add(CreateInputParam("@ApplicationName", SqlDbType.NVarChar, ApplicationName));
                    //cmd.Parameters.Add(CreateInputParam("@EmailToMatch", SqlDbType.NVarChar, emailToMatch));
                    //cmd.Parameters.Add(CreateInputParam("@PageIndex", SqlDbType.Int, pageIndex));
                    //cmd.Parameters.Add(CreateInputParam("@PageSize", SqlDbType.Int, pageSize));
                    //cmd.Parameters.Add(p);
                    try {
                        reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess);
                        while (reader.Read()) {
                            string username, email, passwordQuestion, comment;
                            bool isApproved;
                            DateTime dtCreate, dtLastLogin, dtLastActivity, dtLastPassChange;
                            Guid userId;
                            bool isLockedOut;
                            DateTime dtLastLockoutDate;

                            username = GetNullableString( reader, 0 );
                            email = GetNullableString(reader, 1);
                            passwordQuestion = GetNullableString( reader, 2 );
                            comment = GetNullableString(reader, 3);
                            isApproved = reader.GetBoolean(4);
                            dtCreate = reader.GetDateTime(5).ToLocalTime();
                            dtLastLogin = reader.GetDateTime(6).ToLocalTime();
                            dtLastActivity = reader.GetDateTime(7).ToLocalTime();
                            dtLastPassChange = reader.GetDateTime(8).ToLocalTime();
                            userId = reader.GetGuid( 9 );
                            isLockedOut = reader.GetBoolean( 10 );
                            dtLastLockoutDate = reader.GetDateTime(11).ToLocalTime();

                            users.Add( new MembershipUser( this.Name,
                                                           username,
                                                           userId,
                                                           email,
                                                           passwordQuestion,
                                                           comment,
                                                           isApproved,
                                                           isLockedOut,
                                                           dtCreate,
                                                           dtLastLogin,
                                                           dtLastActivity,
                                                           dtLastPassChange,
                                                           dtLastLockoutDate ) );
                        }

                        return users;
                    }
                    finally
                    {
                        if (reader != null)
                            reader.Close();
                        totalRecords = rowcount;
                    }
                }
                finally
                {
                    if( holder != null )
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            } catch {
                throw;
            }
        }

        /////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////
        private bool CheckPassword( string username, string password, bool updateLastLoginActivityDate, bool failIfNotApproved)
        {
            string              salt;
            int                 passwordFormat;
            return CheckPassword(username, password, updateLastLoginActivityDate, failIfNotApproved, out salt, out passwordFormat);
        }
        private bool CheckPassword( string username, string password, bool updateLastLoginActivityDate, bool failIfNotApproved, out string salt, out int passwordFormat)
        {
            SqlConnectionHolder holder = null;
            string              passwdFromDB;
            int                 status;
            int                 failedPasswordAttemptCount;
            int                 failedPasswordAnswerAttemptCount;
            bool                isPasswordCorrect;
            bool                isApproved;
            DateTime            lastLoginDate, lastActivityDate;

            GetPasswordWithFormat(username, updateLastLoginActivityDate, out status, out passwdFromDB, out passwordFormat, out salt, out failedPasswordAttemptCount,
                                  out failedPasswordAnswerAttemptCount, out isApproved, out lastLoginDate, out lastActivityDate);
            if (status != 0)
                return false;
            if (!isApproved && failIfNotApproved)
                return false;

            string encodedPasswd = EncodePassword( password, passwordFormat, salt );

            isPasswordCorrect = passwdFromDB.Equals( encodedPasswd );

            if( isPasswordCorrect && failedPasswordAttemptCount == 0 && failedPasswordAnswerAttemptCount == 0 )
                return true;

            try
            {
                try
                {
                    holder = SqlConnectionHelper.GetConnection(_sqlConnectionString, true);
                    CheckSchemaVersion( holder.Connection );


                    status = MySqlStoredProcedures.aspnet_Membership_UpdateUserInfo(ApplicationName, username,
                            isPasswordCorrect, updateLastLoginActivityDate, MaxInvalidPasswordAttempts,
                            PasswordAttemptWindow, DateTime.UtcNow, lastLoginDate, lastActivityDate,
                            holder);

                    //MySqlCommand cmd = new MySqlCommand( "dbo.aspnet_Membership_UpdateUserInfo", holder.Connection );
                    //DateTime   dtNow = DateTime.UtcNow;
                    //cmd.CommandTimeout = CommandTimeout;
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add( CreateInputParam( "@ApplicationName", SqlDbType.NVarChar, ApplicationName ) );
                    //cmd.Parameters.Add( CreateInputParam( "@UserName", SqlDbType.NVarChar, username ) );
                    //cmd.Parameters.Add( CreateInputParam( "@IsPasswordCorrect", SqlDbType.Bit, isPasswordCorrect ) );
                    //cmd.Parameters.Add( CreateInputParam( "@UpdateLastLoginActivityDate", SqlDbType.Bit, updateLastLoginActivityDate ) );
                    //cmd.Parameters.Add( CreateInputParam( "@MaxInvalidPasswordAttempts", SqlDbType.Int, MaxInvalidPasswordAttempts ) );
                    //cmd.Parameters.Add( CreateInputParam( "@PasswordAttemptWindow", SqlDbType.Int, PasswordAttemptWindow ) );
                    //cmd.Parameters.Add( CreateInputParam( "@CurrentTimeUtc", SqlDbType.DateTime, dtNow));
                    //cmd.Parameters.Add( CreateInputParam( "@LastLoginDate", SqlDbType.DateTime, isPasswordCorrect ? dtNow : lastLoginDate));
                    //cmd.Parameters.Add( CreateInputParam( "@LastActivityDate", SqlDbType.DateTime, isPasswordCorrect ? dtNow : lastActivityDate));
                    //MySqlParameter p = new MySqlParameter("@ReturnValue", SqlDbType.Int);
                    //p.Direction = ParameterDirection.ReturnValue;
                    //cmd.Parameters.Add(p);

                    //cmd.ExecuteNonQuery();

                    //status = ( ( p.Value != null ) ? ( ( int )p.Value ) : -1 );
                }
                finally
                {
                    if( holder != null )
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            }
            catch
            {
                throw;
            }

            return isPasswordCorrect;
        }

        /////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////
        private void GetPasswordWithFormat( string       username,
                                            bool         updateLastLoginActivityDate,
                                            out int      status,
                                            out string   password,
                                            out int      passwordFormat,
                                            out string   passwordSalt,
                                            out int      failedPasswordAttemptCount,
                                            out int      failedPasswordAnswerAttemptCount,
                                            out bool     isApproved,
                                            out DateTime lastLoginDate,
                                            out DateTime lastActivityDate)
        {
            try {
                SqlConnectionHolder holder = null;
                MySqlDataReader       reader = null;
                //MySqlParameter        p      = null;

                try
                {
                    holder = SqlConnectionHelper.GetConnection(_sqlConnectionString, true);
                    CheckSchemaVersion(holder.Connection);

                    MySqlCommand cmd = new MySqlCommand();

                    status = MySqlStoredProcedures.aspnet_Membership_GetPasswordWithFormat(ApplicationName,
                            username, updateLastLoginActivityDate, DateTime.UtcNow, holder, ref cmd);



                    //MySqlCommand    cmd     = new MySqlCommand( "dbo.aspnet_Membership_GetPasswordWithFormat", holder.Connection );

                    //cmd.CommandTimeout = CommandTimeout;
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add(CreateInputParam( "@ApplicationName", SqlDbType.NVarChar, ApplicationName ) );
                    //cmd.Parameters.Add(CreateInputParam( "@UserName", SqlDbType.NVarChar, username ) );
                    //cmd.Parameters.Add(CreateInputParam("@UpdateLastLoginActivityDate", SqlDbType.Bit, updateLastLoginActivityDate));
                    //cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", SqlDbType.DateTime, DateTime.UtcNow));

                    //p = new MySqlParameter( "@ReturnValue", SqlDbType.Int );
                    //p.Direction = ParameterDirection.ReturnValue;
                    //cmd.Parameters.Add(p);
                    
                    reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

                    //status = -1;

                    if (reader.Read())
                    {
                        password = reader.GetString(0);
                        passwordFormat = reader.GetInt32(1);
                        passwordSalt = reader.GetString(2);
                        failedPasswordAttemptCount = reader.GetInt32(3);
                        failedPasswordAnswerAttemptCount = reader.GetInt32(4);
                        isApproved = reader.GetBoolean(5);
                        lastLoginDate = reader.GetDateTime(6);
                        lastActivityDate = reader.GetDateTime(7);
                    }
                    else
                    {
                        password = null;
                        passwordFormat = 0;
                        passwordSalt = null;
                        failedPasswordAttemptCount = 0;
                        failedPasswordAnswerAttemptCount = 0;
                        isApproved = false;
                        lastLoginDate = DateTime.UtcNow;
                        lastActivityDate = DateTime.UtcNow;
                    }
                }
                finally
                {
                    if( reader != null )
                    {
                        reader.Close();
                        reader = null;

                        //status = ( ( p.Value != null ) ? ( ( int )p.Value ) : -1 );
                    }

                    if( holder != null )
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            } catch
            {
                throw;
            }

        }

        /////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////
        private string GetPasswordFromDB( string       username,
                                          string       passwordAnswer,
                                          bool         requiresQuestionAndAnswer,
                                          out int      passwordFormat,
                                          out int      status )
        {
            try {
                SqlConnectionHolder holder = null;
                MySqlDataReader       reader = null;
                //MySqlParameter        p       = null;

                try {
                    holder = SqlConnectionHelper.GetConnection( _sqlConnectionString, true );
                    CheckSchemaVersion( holder.Connection );


                    MySqlCommand cmd = new MySqlCommand();

                    status = MySqlStoredProcedures.aspnet_Membership_GetPassword(ApplicationName,
                            username, MaxInvalidPasswordAttempts, PasswordAttemptWindow, DateTime.UtcNow,
                            ((requiresQuestionAndAnswer) ? (passwordAnswer) : null), holder, ref cmd);



                    //MySqlCommand cmd = new MySqlCommand( "dbo.aspnet_Membership_GetPassword", holder.Connection );

                    //cmd.CommandTimeout = CommandTimeout;
                    //cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add( CreateInputParam( "@ApplicationName", SqlDbType.NVarChar, ApplicationName ) );
                    //cmd.Parameters.Add( CreateInputParam( "@UserName", SqlDbType.NVarChar, username ) );
                    //cmd.Parameters.Add( CreateInputParam( "@MaxInvalidPasswordAttempts", SqlDbType.Int, MaxInvalidPasswordAttempts ) );
                    //cmd.Parameters.Add( CreateInputParam( "@PasswordAttemptWindow", SqlDbType.Int, PasswordAttemptWindow ) );
                    //cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", SqlDbType.DateTime, DateTime.UtcNow));

                    //if ( requiresQuestionAndAnswer )
                    //{
                    //    cmd.Parameters.Add( CreateInputParam( "@PasswordAnswer", SqlDbType.NVarChar, passwordAnswer ) );
                    //}

                    //p = new MySqlParameter( "@ReturnValue", SqlDbType.Int );
                    //p.Direction = ParameterDirection.ReturnValue;
                    //cmd.Parameters.Add(p);

                    reader = cmd.ExecuteReader( CommandBehavior.SingleRow );

                    string password = null;

                    //status = -1;

                    if ( reader.Read() )
                    {
                        password = reader.GetString( 0 );
                        passwordFormat = reader.GetInt32( 1 );
                    }
                    else
                    {
                        password = null;
                        passwordFormat = 0;
                    }

                    return password;
                }
                finally
                {
                    if( reader != null )
                    {
                        reader.Close();
                        reader = null;

                        //status = ( ( p.Value != null ) ? ( ( int )p.Value ) : -1 );
                    }

                    if( holder != null )
                    {
                        holder.Close();
                        holder = null;
                    }
                }
            }
            catch
            {
                throw;
            }

        }

        /////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////
        private string GetEncodedPasswordAnswer(string username, string passwordAnswer)
        {
            if( passwordAnswer != null )
            {
                passwordAnswer = passwordAnswer.Trim();
            }
            if (string.IsNullOrEmpty(passwordAnswer))
                return passwordAnswer;
            int status, passwordFormat, failedPasswordAttemptCount, failedPasswordAnswerAttemptCount;
            string password, passwordSalt;
            bool isApproved;
            DateTime lastLoginDate, lastActivityDate;
            GetPasswordWithFormat(username, false, out status, out password, out passwordFormat, out passwordSalt,
                                  out failedPasswordAttemptCount, out failedPasswordAnswerAttemptCount, out isApproved, out lastLoginDate, out lastActivityDate);
            if (status == 0)
                return EncodePassword(passwordAnswer.ToLower(CultureInfo.InvariantCulture), passwordFormat, passwordSalt);
            else
                throw new ProviderException(GetExceptionText(status));
        }

        /////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////

        public virtual string GeneratePassword()
        {
            return Membership.GeneratePassword(
                      MinRequiredPasswordLength < PASSWORD_SIZE ? PASSWORD_SIZE : MinRequiredPasswordLength,
                      MinRequiredNonAlphanumericCharacters );
        }

        /////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////
        private MySqlParameter CreateInputParam( string paramName,
                                               MySqlDbType dbType,
                                               object objValue )
        {

            MySqlParameter param = new MySqlParameter( paramName, dbType );

            if( objValue == null )
            {
                param.IsNullable = true;
                param.Value = DBNull.Value;
            }
            else
            {
                param.Value = objValue;
            }

            return param;
        }

        /////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////
        private string GetNullableString(MySqlDataReader reader, int col)
        {
            if( reader.IsDBNull( col ) == false )
            {
                return reader.GetString( col );
            }

            return null;
        }
        /////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////
        private string GetExceptionText(int status) {
            string key;
            switch (status)
            {
            case 0:
                return String.Empty;
            case 1:
                key = SR.Membership_UserNotFound;
                break;
            case 2:
                key = SR.Membership_WrongPassword;
                break;
            case 3:
                key = SR.Membership_WrongAnswer;
                break;
            case 4:
                key = SR.Membership_InvalidPassword;
                break;
            case 5:
                key = SR.Membership_InvalidQuestion;
                break;
            case 6:
                key = SR.Membership_InvalidAnswer;
                break;
            case 7:
                key = SR.Membership_InvalidEmail;
                break;
            case 99:
                key = SR.Membership_AccountLockOut;
                break;
            default:
                key = SR.Provider_Error;
                break;
            }
            return SR.GetString(key);
        }

        private bool IsStatusDueToBadPassword( int status )
        {
            return ( status >= 2 && status <= 6 || status == 99 );
        }

        private DateTime RoundToSeconds(DateTime dt)
        {
            return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second);
        }
        internal string GenerateSalt()
        {
            byte[] buf = new byte[16];
            (new RNGCryptoServiceProvider()).GetBytes(buf);
            return Convert.ToBase64String(buf);
        }
        internal string EncodePassword(string pass, int passwordFormat, string salt)
        {
            if (passwordFormat == 0) // MembershipPasswordFormat.Clear
                return pass;

            byte[] bIn = Encoding.Unicode.GetBytes(pass);
            byte[] bSalt = Convert.FromBase64String(salt);
            byte[] bAll = new byte[bSalt.Length + bIn.Length];
            byte[] bRet = null;

            Buffer.BlockCopy(bSalt, 0, bAll, 0, bSalt.Length);
            Buffer.BlockCopy(bIn, 0, bAll, bSalt.Length, bIn.Length);
            if (passwordFormat == 1)
            { // MembershipPasswordFormat.Hashed
                HashAlgorithm s = HashAlgorithm.Create( Membership.HashAlgorithmType );
                bRet = s.ComputeHash(bAll);
            } else
            {
                bRet = EncryptPassword( bAll );
            }

            return Convert.ToBase64String(bRet);
        }

        internal string UnEncodePassword(string pass, int passwordFormat)
        {
            switch (passwordFormat)
            {
                case 0: // MembershipPasswordFormat.Clear:
                    return pass;
                case 1: // MembershipPasswordFormat.Hashed:
                    throw new ProviderException(SR.GetString(SR.Provider_can_not_decode_hashed_password));
                default:
                    byte[] bIn = Convert.FromBase64String(pass);
                    byte[] bRet = DecryptPassword( bIn );
                    if (bRet == null)
                        return null;
                    return Encoding.Unicode.GetString(bRet, 16, bRet.Length - 16);
            }
        }
    }
    /////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////
}
