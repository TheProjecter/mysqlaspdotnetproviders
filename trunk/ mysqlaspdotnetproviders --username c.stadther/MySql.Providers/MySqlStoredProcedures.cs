namespace MySql.Providers {
    using System;
    using System.Collections.Generic;
    using MySql.Data.MySqlClient; 
    using System.Text;
    using System.Data;


    internal static class MySqlStoredProcedures
    {
        internal static Guid aspnet_Applications_CreateApplication(SqlConnectionHolder holder, String ApplicationName)
        {
            Guid ApplicationId = Guid.Empty;
            
            MySqlTransaction sqlTrans = null;

            String sQuery = "SELECT ApplicationId FROM aspnet_Applications " +
                "WHERE @ApplicationName = LoweredApplicationName";

            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader       reader = null;
            reader = cmd.ExecuteReader( CommandBehavior.SingleRow );

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
           

            if (ApplicationId.Equals(Guid.Empty))
            {
                try
                {
                    sqlTrans = holder.Connection.BeginTransaction();

                    ApplicationId = Guid.NewGuid();

                    sQuery = "INSERT  aspnet_Applications (ApplicationId, ApplicationName, LoweredApplicationName)" +
                        "VALUES  (@ApplicationId, @ApplicationName, @LowerApplicationName)";

                    cmd = new MySqlCommand(sQuery, holder.Connection);
                    cmd.Transaction = sqlTrans;

                    cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
                    cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName));
                    cmd.Parameters.Add(CreateInputParam("@LowerApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

                    cmd.ExecuteNonQuery();
                    sqlTrans.Commit();
                }
                catch
                {
                    try
                    {
                        sqlTrans.Rollback();
                    }
                    catch
                    {
                        throw;
                    }
                }
            }
            
            return ApplicationId;
        }

        internal static int aspnet_Membership_CreateUser(String ApplicationName, String UserName, String Password, 
                String PasswordSalt, String Email, String PasswordQuestion, String PasswordAnswer, Boolean IsApproved,
                DateTime CurrentTimeUtc, DateTime CreateDate, Boolean UniqueEmail, int PasswordFormat, ref Object UserId,
                SqlConnectionHolder holder)
        {
            MySqlTransaction sqlTrans = null;

            Guid ApplicationId = Guid.Empty;
            Guid NewUserId = Guid.Empty;
            Boolean IsLockedOut = false;
            DateTime LastLockoutDate = new DateTime(1754, 1, 1, 0, 0, 0);
            int FailedPasswordAttemptCount = 0;
            DateTime FailedPasswordAttemptWindowStart = new DateTime(1754, 1, 1, 0, 0, 0);
            int FailedPasswordAnswerAttemptCount = 0;
            DateTime FailedPasswordAnswerAttemptWindowStart = new DateTime(1754, 1, 1, 0, 0, 0);

            ApplicationId = aspnet_Applications_CreateApplication(holder, ApplicationName);

            CreateDate = CurrentTimeUtc;

            String sQuery = "SELECT UserId FROM aspnet_Users WHERE @UserName = LoweredUserName AND @ApplicationId = ApplicationId";

            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    NewUserId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (!NewUserId.Equals(Guid.Empty))
            {
                return 6;
            }
            else
            {
                NewUserId = aspnet_Users_CreateUser(ApplicationId, UserName, false, CreateDate, holder); 
            }
            
            if (UniqueEmail == true) 
            {
                sQuery = "SELECT Count(UserId) FROM aspnet_Membership WHERE ApplicationId = @ApplicationId AND LoweredEmail = @Email";

                cmd = new MySqlCommand(sQuery, holder.Connection);
                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
                cmd.Parameters.Add(CreateInputParam("@Email", MySqlDbType.VarChar, Email.ToLower()));

                if ((long)cmd.ExecuteScalar() > 0)
                {
                    return 7;
                }
            }

            try
            {
                sqlTrans = holder.Connection.BeginTransaction();

                sQuery = "INSERT INTO aspnet_Membership " +
                    "( ApplicationId," +
                    "UserId," +
                    "Password," +
                    "PasswordSalt," +
                    "Email," +
                    "LoweredEmail," +
                    "PasswordQuestion," +
                    "PasswordAnswer," +
                    "PasswordFormat," +
                    "IsApproved," +
                    "IsLockedOut," +
                    "CreateDate," +
                    "LastLoginDate," +
                    "LastPasswordChangedDate," +
                    "LastLockoutDate," +
                    "FailedPasswordAttemptCount," +
                    "FailedPasswordAttemptWindowStart," +
                    "FailedPasswordAnswerAttemptCount," +
                    "FailedPasswordAnswerAttemptWindowStart ) " +
                    "VALUES ( @ApplicationId," +
                    "@UserId," +
                    "@Password," +
                    "@PasswordSalt," +
                    "@Email," +
                    "@LowerEmail," +
                    "@PasswordQuestion," +
                    "@PasswordAnswer," +
                    "@PasswordFormat," +
                    "@IsApproved," +
                    "@IsLockedOut," +
                    "@CreateDate," +
                    "@CreateDate," +
                    "@CreateDate," +
                    "@LastLockoutDate," +
                    "@FailedPasswordAttemptCount," +
                    "@FailedPasswordAttemptWindowStart," +
                    "@FailedPasswordAnswerAttemptCount," +
                    "@FailedPasswordAnswerAttemptWindowStart )";

                cmd = new MySqlCommand(sQuery, holder.Connection);
                cmd.Transaction = sqlTrans;

                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
                cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, NewUserId));
                cmd.Parameters.Add(CreateInputParam("@Password", MySqlDbType.VarChar, Password));
                cmd.Parameters.Add(CreateInputParam("@PasswordSalt", MySqlDbType.VarChar, PasswordSalt));
                cmd.Parameters.Add(CreateInputParam("@Email", MySqlDbType.VarChar, Email));
                cmd.Parameters.Add(CreateInputParam("@LowerEmail", MySqlDbType.VarChar, Email.ToLower()));
                cmd.Parameters.Add(CreateInputParam("@PasswordQuestion", MySqlDbType.VarChar, PasswordQuestion));
                cmd.Parameters.Add(CreateInputParam("@PasswordAnswer", MySqlDbType.VarChar, PasswordAnswer));
                cmd.Parameters.Add(CreateInputParam("@PasswordFormat", MySqlDbType.Int16, PasswordFormat));
                cmd.Parameters.Add(CreateInputParam("@IsApproved", MySqlDbType.Bit, IsApproved));
                cmd.Parameters.Add(CreateInputParam("@IsLockedOut", MySqlDbType.Bit, IsLockedOut));
                cmd.Parameters.Add(CreateInputParam("@CreateDate", MySqlDbType.DateTime, CreateDate));

                cmd.Parameters.Add(CreateInputParam("@LastLockoutDate", MySqlDbType.DateTime, LastLockoutDate));
                cmd.Parameters.Add(CreateInputParam("@FailedPasswordAttemptCount", MySqlDbType.Int16, FailedPasswordAttemptCount));
                cmd.Parameters.Add(CreateInputParam("@FailedPasswordAttemptWindowStart", MySqlDbType.DateTime, FailedPasswordAttemptWindowStart));
                cmd.Parameters.Add(CreateInputParam("@FailedPasswordAnswerAttemptCount", MySqlDbType.Int16, FailedPasswordAnswerAttemptCount));
                cmd.Parameters.Add(CreateInputParam("@FailedPasswordAnswerAttemptWindowStart", MySqlDbType.DateTime, FailedPasswordAnswerAttemptWindowStart));

                cmd.ExecuteNonQuery();
                sqlTrans.Commit(); 
            }
            catch
            {
                try
                {
                    sqlTrans.Rollback();
                    return -1;
                }
                catch
                {
                    throw;
                }
            }
            UserId = NewUserId;

            return 0;
        }

        internal static Guid aspnet_Users_CreateUser(Guid ApplicationId, String UserName, Boolean IsUserAnonymous,
                DateTime LastActivityDate, SqlConnectionHolder holder)
        {
            MySqlTransaction sqlTrans = null;

            Guid NewUserId = Guid.NewGuid();

            try
            {
                sqlTrans = holder.Connection.BeginTransaction();
                String sQuery = "INSERT aspnet_Users " +
                   "( ApplicationId, " + 
                    "UserId, " + 
                    "UserName, " + 
                    "LoweredUserName, " +
                    "IsAnonymous,  " + 
                    "LastActivityDate)" + 
                    "VALUES (@ApplicationId, " + 
                    "@UserId, " + 
                    "@UserName, " +
                    "@LowerUserName, " +
                    "@IsUserAnonymous, " +
                    "@LastActivityDate)";

                MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
                cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, NewUserId));
                cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName));
                cmd.Parameters.Add(CreateInputParam("@LowerUserName", MySqlDbType.VarChar, UserName.ToLower()));
                cmd.Parameters.Add(CreateInputParam("@IsUserAnonymous", MySqlDbType.Bit, IsUserAnonymous));
                cmd.Parameters.Add(CreateInputParam("@LastActivityDate", MySqlDbType.DateTime, LastActivityDate));

                cmd.ExecuteScalar();
                sqlTrans.Commit();
            }
            catch
            {
                try
                {
                    sqlTrans.Rollback();
                }
                catch
                {
                    throw;
                }
            }

            return NewUserId;
        }


        internal static int aspnet_Membership_ChangePasswordQuestionAndAnswer(String ApplicationName, String UserName,
                String NewPasswordQuestion, String NewPasswordAnswer, SqlConnectionHolder holder)
        {
            Guid UserId = Guid.Empty;

            MySqlTransaction sqlTrans = null;

            String sQuery = "SELECT aspnet_Users.UserId FROM aspnet_Membership, aspnet_Users, aspnet_Applications " +
                "WHERE aspnet_Users.LoweredUserName = @UserName AND aspnet_users.ApplicationId = " +
                "aspnet_applications.ApplicationId AND @ApplicationName = aspnet_Applications.LoweredApplicationName " +
                "AND aspnet_Users.UserId = aspnet_Membership.UserId";
            
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (UserId.Equals(Guid.Empty)) 
            {
                return 1;
            }
            
            
            try
            {
                sqlTrans = holder.Connection.BeginTransaction();
                sQuery = "UPDATE aspnet_Membership SET PasswordQuestion = @NewPasswordQuestion, " +
                    "PasswordAnswer = @NewPasswordAnswer WHERE  UserId=@UserId";

                cmd = new MySqlCommand(sQuery, holder.Connection);

                cmd.Parameters.Add(CreateInputParam("@NewPasswordQuestion", MySqlDbType.VarChar, NewPasswordQuestion));
                cmd.Parameters.Add(CreateInputParam("@NewPasswordAnswer", MySqlDbType.VarChar, NewPasswordAnswer));
                cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                cmd.ExecuteNonQuery();
                sqlTrans.Commit(); 

            }
            catch 
            {
                try
                {
                    sqlTrans.Rollback();
                    return -1;
                }
                catch 
                {
                    throw;
                }
            }

            return 0;
        }
    
        internal static int aspnet_Membership_SetPassword(String ApplicationName, String UserName, String NewPassword,
                String PasswordSalt, DateTime CurrentTimeUtc, int PasswordFormat, SqlConnectionHolder holder)
        {
            Guid UserId = Guid.Empty; 

            MySqlTransaction sqlTrans = null;

            String sQuery = "SELECT aspnet_Users.userid FROM aspnet_Membership, aspnet_Users, aspnet_Applications " +
                "WHERE aspnet_Users.LoweredUserName = @UserName AND aspnet_users.ApplicationId = " +
                "aspnet_applications.ApplicationId AND @ApplicationName = aspnet_Applications.LoweredApplicationName " +
                "AND aspnet_Users.UserId = aspnet_Membership.UserId";

            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (UserId.Equals(Guid.Empty)) 
            {
                return 1;
            }


            try
            {
                sqlTrans = holder.Connection.BeginTransaction();
                sQuery = "UPDATE aspnet_Membership SET Password = @NewPassword, PasswordFormat = @PasswordFormat, " +
                    "PasswordSalt = @PasswordSalt, LastPasswordChangedDate = @CurrentTimeUtc WHERE UserId=@UserId";

                cmd = new MySqlCommand(sQuery, holder.Connection);

                cmd.Parameters.Add(CreateInputParam("@NewPassword", MySqlDbType.VarChar, NewPassword));
                cmd.Parameters.Add(CreateInputParam("@PasswordFormat", MySqlDbType.Int16, PasswordFormat));
                cmd.Parameters.Add(CreateInputParam("@PasswordSalt", MySqlDbType.VarChar, PasswordSalt));
                cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", MySqlDbType.DateTime, CurrentTimeUtc));
                cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
                                
                cmd.ExecuteNonQuery();
                sqlTrans.Commit();
            }
            catch 
            {
                try
                {
                    sqlTrans.Rollback();
                    return -1;
                }
                catch 
                {
                    throw;
                }
            }

            return 0;
        }


        internal static int aspnet_Membership_ResetPassword(String ApplicationName, String UserName, String NewPassword,
                int MaxInvalidPasswordAttempts, int PasswordAttemptWindow, String PasswordSalt, DateTime CurrentTimeUtc,
                int PasswordFormat, String PasswordAnswer, SqlConnectionHolder holder)
        { 
            Boolean IsLockedOut = false;
            DateTime LastLockoutDate = new DateTime();
            int FailedPasswordAttemptCount = 0;
            DateTime FailedPasswordAttemptWindowStart = new DateTime(1754, 1, 1, 0, 0, 0); ;
            int FailedPasswordAnswerAttemptCount = 0;
            DateTime FailedPasswordAnswerAttemptWindowStart = new DateTime(1754, 1, 1, 0, 0, 0); ;

            Guid UserId = Guid.Empty;

            MySqlTransaction sqlTrans = null;

            String sQuery = "SELECT aspnet_Users.userid FROM aspnet_Membership, aspnet_Users, aspnet_Applications " +
                "WHERE aspnet_Users.LoweredUserName = @UserName AND aspnet_users.ApplicationId = " +
                "aspnet_applications.ApplicationId AND @ApplicationName = aspnet_Applications.LoweredApplicationName " +
                "AND aspnet_Users.UserId = aspnet_Membership.UserId";


            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (UserId.Equals(Guid.Empty))
            {
                return 1;
            }

            sQuery = "SELECT IsLockedOut, " +
                "LastLockoutDate, " +
                "FailedPasswordAttemptCount, " +
                "FailedPasswordAttemptWindowStart, " +
                "FailedPasswordAnswerAttemptCount, " +
                "FailedPasswordAnswerAttemptWindowStart " +
                "FROM aspnet_Membership " +
                "WHERE @UserId = UserId ";

            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
            
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    IsLockedOut = reader.GetBoolean(0);
                    LastLockoutDate = reader.GetDateTime(1);
                    FailedPasswordAttemptCount = reader.GetInt32(2);
                    FailedPasswordAttemptWindowStart = reader.GetDateTime(3);
                    FailedPasswordAnswerAttemptCount = reader.GetInt32(4);
                    FailedPasswordAnswerAttemptWindowStart = reader.GetDateTime(5);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            
            if (IsLockedOut == true)
            {
                return 99;
            }

            try
            {
                sqlTrans = holder.Connection.BeginTransaction();
                sQuery = "UPDATE aspnet_Membership SET Password = @NewPassword, LastPasswordChangedDate = @CurrentTimeUtc, " +
                        "PasswordFormat = @PasswordFormat,PasswordSalt = @PasswordSalt WHERE  @UserId = UserId AND " +
                        "( ( @PasswordAnswer IS NULL ) OR ( LOWER( PasswordAnswer ) = LOWER( @PasswordAnswer ) ) )";

                cmd = new MySqlCommand(sQuery, holder.Connection);
                cmd.Transaction = sqlTrans;

                cmd.Parameters.Add(CreateInputParam("@NewPassword", MySqlDbType.VarChar, NewPassword));
                cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", MySqlDbType.DateTime, CurrentTimeUtc));
                cmd.Parameters.Add(CreateInputParam("@PasswordFormat", MySqlDbType.Int32, PasswordFormat));
                cmd.Parameters.Add(CreateInputParam("@PasswordSalt", MySqlDbType.VarChar, PasswordSalt));
                cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
                cmd.Parameters.Add(CreateInputParam("@PasswordAnswer", MySqlDbType.VarChar, PasswordAnswer));

                int rowCount = cmd.ExecuteNonQuery();
                sqlTrans.Commit();

                if (rowCount == 0)
                {
                    if (CurrentTimeUtc > FailedPasswordAnswerAttemptWindowStart.AddMinutes(PasswordAttemptWindow))
                    {
                        FailedPasswordAnswerAttemptWindowStart = CurrentTimeUtc;
                        FailedPasswordAnswerAttemptCount = 1;
                    }
                    else
                    {
                        FailedPasswordAnswerAttemptWindowStart = CurrentTimeUtc;
                        FailedPasswordAnswerAttemptCount++;
                    }

                    if (FailedPasswordAnswerAttemptCount > MaxInvalidPasswordAttempts)
                    {
                        IsLockedOut = true;
                        LastLockoutDate = CurrentTimeUtc;
                    }

                    return 3;
                }
                else
                {
                    if (FailedPasswordAnswerAttemptCount > 0) 
                    {
                        FailedPasswordAnswerAttemptCount = 0;
                        FailedPasswordAnswerAttemptWindowStart = new DateTime(1754, 1, 1, 0, 0, 0);
                    }
                    
                }

                if (PasswordAnswer != null)
                {
                    sqlTrans = holder.Connection.BeginTransaction();

                    sQuery = "UPDATE aspnet_Membership SET IsLockedOut = @IsLockedOut, " +
                        "LastLockoutDate = @LastLockoutDate, FailedPasswordAttemptCount = " +
                        "@FailedPasswordAttemptCount, FailedPasswordAttemptWindowStart = " +
                        "@FailedPasswordAttemptWindowStart, FailedPasswordAnswerAttemptCount = " +
                        "@FailedPasswordAnswerAttemptCount, FailedPasswordAnswerAttemptWindowStart = " +
                        "@FailedPasswordAnswerAttemptWindowStart WHERE @UserId = UserId";

                    cmd = new MySqlCommand(sQuery, holder.Connection);
                    cmd.Transaction = sqlTrans;

                    cmd.Parameters.Add(CreateInputParam("@IsLockedOut", MySqlDbType.Bit, IsLockedOut));
                    cmd.Parameters.Add(CreateInputParam("@LastLockoutDate", MySqlDbType.DateTime, LastLockoutDate));
                    cmd.Parameters.Add(CreateInputParam("@FailedPasswordAttemptCount", MySqlDbType.Int32, FailedPasswordAttemptCount));
                    cmd.Parameters.Add(CreateInputParam("@FailedPasswordAttemptWindowStart", MySqlDbType.DateTime, FailedPasswordAttemptWindowStart));
                    cmd.Parameters.Add(CreateInputParam("@FailedPasswordAnswerAttemptCount", MySqlDbType.Int32, FailedPasswordAnswerAttemptCount));
                    cmd.Parameters.Add(CreateInputParam("@FailedPasswordAnswerAttemptWindowStart", MySqlDbType.DateTime, FailedPasswordAnswerAttemptWindowStart));
                    cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                    cmd.ExecuteNonQuery();
                    sqlTrans.Commit();
                }
            }
            catch 
            {
                try
                {
                    sqlTrans.Rollback();
                    return -1;
                }
                catch 
                {
                    throw;
                }
            }
            return 0;
        }


        internal static int aspnet_Membership_UpdateUser(String ApplicationName, String UserName, String Email,
                String Comment, Boolean IsApproved, DateTime LastLoginDate, DateTime LastActivityDate,
                Boolean UniqueEmail, DateTime CurrentTimeUtc, SqlConnectionHolder holder)
        {

            Guid UserId = Guid.Empty;
            Guid ApplicationId = Guid.Empty;

            MySqlTransaction sqlTrans = null;

            String sQuery = "SELECT aspnet_Users.UserId, aspnet_Applications.ApplicationId FROM aspnet_Membership, aspnet_Users, aspnet_Applications " +
                "WHERE aspnet_Users.LoweredUserName = @UserName AND aspnet_users.ApplicationId = " +
                "aspnet_applications.ApplicationId AND @ApplicationName = aspnet_Applications.LoweredApplicationName " +
                "AND aspnet_Users.UserId = aspnet_Membership.UserId";
            
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                    ApplicationId = reader.GetGuid(1);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            
            if (UserId.Equals(Guid.Empty) || ApplicationId.Equals(Guid.Empty))
            {
                return 1;
            }
                                    
            if (UniqueEmail == true)
            {
                sQuery = "SELECT UserId FROM aspnet_Membership WHERE ApplicationId = @ApplicationId AND LoweredEmail = @Email";

                cmd = new MySqlCommand(sQuery, holder.Connection);
                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
                cmd.Parameters.Add(CreateInputParam("@Email", MySqlDbType.VarChar, Email.ToLower()));

                Guid _UserId = Guid.Empty;

                reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

                try
                {
                    if (reader.Read())
                    {
                        _UserId = reader.GetGuid(0);
                    }
                }
                catch
                {

                }
                finally
                {
                    if (reader != null)
                    {
                        reader.Close();
                        reader = null;
                    }
                }

                if (!_UserId.Equals(UserId) && !_UserId.Equals(Guid.Empty))
                {
                    return 7;
                }
            }

            try
            {
                sqlTrans = holder.Connection.BeginTransaction();

                sQuery = "UPDATE aspnet_Users SET LastActivityDate = @LastActivityDate WHERE @UserId = UserId";

                cmd = new MySqlCommand(sQuery, holder.Connection);
                cmd.Transaction = sqlTrans;

                cmd.Parameters.Add(CreateInputParam("@LastActivityDate", MySqlDbType.DateTime, LastActivityDate));
                cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
                
                cmd.ExecuteNonQuery();
                
                sQuery = "UPDATE aspnet_Membership SET Email = @Email, " +
                    "LoweredEmail     = @LowerEmail, " +
                    "Comment          = @Comment, " +
                    "IsApproved       = @IsApproved, " +
                    "LastLoginDate    = @LastLoginDate " +
                    "WHERE @UserId = UserId ";

                cmd = new MySqlCommand(sQuery, holder.Connection);
                cmd.Transaction = sqlTrans;

                cmd.Parameters.Add(CreateInputParam("@Email", MySqlDbType.VarChar, Email));
                cmd.Parameters.Add(CreateInputParam("@LowerEmail", MySqlDbType.VarChar, Email.ToLower()));
                cmd.Parameters.Add(CreateInputParam("@Comment", MySqlDbType.Text, Comment));
                cmd.Parameters.Add(CreateInputParam("@IsApproved", MySqlDbType.Bit, IsApproved));
                cmd.Parameters.Add(CreateInputParam("@LastLoginDate", MySqlDbType.DateTime, LastLoginDate));
                cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                cmd.ExecuteNonQuery();
                sqlTrans.Commit();
            }
            catch 
            {
                try
                {
                    sqlTrans.Rollback();
                    return -1;
                }
                catch 
                {
                    throw;
                }
            }
            return 0;
        }


        internal static int aspnet_Membership_UnlockUser(String ApplicationName, String UserName, 
                SqlConnectionHolder holder)
        {
            Guid UserId = Guid.Empty;

            MySqlTransaction sqlTrans = null;

            String sQuery = "SELECT aspnet_Users.userid FROM aspnet_Membership, aspnet_Users, aspnet_Applications " +
                "WHERE aspnet_Users.LoweredUserName = @UserName AND aspnet_users.ApplicationId = " +
                "aspnet_applications.ApplicationId AND @ApplicationName = aspnet_Applications.LoweredApplicationName " +
                "AND aspnet_Users.UserId = aspnet_Membership.UserId";

            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
                       
            if (UserId.Equals(Guid.Empty)) 
            {
                return 1;
            }

            try
            {
                sqlTrans = holder.Connection.BeginTransaction();

                sQuery = "UPDATE aspnet_Membership " +
                    "SET IsLockedOut = 0, " +
                    "FailedPasswordAttemptCount = 0, " +
                    "FailedPasswordAttemptWindowStart = STR_TO_DATE('01/01/1754', '%m/%d/%Y') , " +
                    "FailedPasswordAnswerAttemptCount = 0, " +
                    "FailedPasswordAnswerAttemptWindowStart = STR_TO_DATE('01/01/1754', '%m/%d/%Y'), " +
                    "LastLockoutDate = STR_TO_DATE('01/01/1754', '%m/%d/%Y') " +
                    "WHERE @UserId = UserId";
            
                cmd = new MySqlCommand(sQuery, holder.Connection);
                                
                cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
                                
                cmd.ExecuteNonQuery();
                sqlTrans.Commit();
            }
            catch 
            {
                try
                {
                    sqlTrans.Rollback();
                    return -1;
                }
                catch 
                {
                    throw;
                }
            }
            return 0;
        }

        internal static int aspnet_Users_DeleteUser(String ApplicationName, String UserName, 
                int TablesToDeleteFrom, ref int NumTablesDeletedFrom, SqlConnectionHolder holder)
        {
            Guid UserId = Guid.Empty;
            NumTablesDeletedFrom = 0;

            MySqlTransaction sqlTrans = null;

            String sQuery = "SELECT aspnet_Users.userid FROM aspnet_Membership, aspnet_Users, aspnet_Applications " +
                "WHERE aspnet_Users.LoweredUserName = @UserName AND aspnet_users.ApplicationId = " +
                "aspnet_applications.ApplicationId AND @ApplicationName = aspnet_Applications.LoweredApplicationName " +
                "AND aspnet_Users.UserId = aspnet_Membership.UserId";

            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }


            if (UserId.Equals(Guid.Empty))
            {
                return 1;
            }

            try
            {
                sqlTrans = holder.Connection.BeginTransaction();

                // Delete from Membership table if (@TablesToDeleteFrom & 1) is set
                if ((TablesToDeleteFrom & 1) > 0)
                {
                    try
                    {
                        sQuery = "DELETE FROM aspnet_Membership WHERE @UserId = UserId";
                        cmd = new MySqlCommand(sQuery, holder.Connection);
                        cmd.Transaction = sqlTrans;

                        cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                        if (cmd.ExecuteNonQuery() > 0)
                        {
                            NumTablesDeletedFrom++;
                        }
                    }
                    catch
                    {

                    }
                }

                //Delete from aspnet_UsersInRoles table if (@TablesToDeleteFrom & 2) is set
                if ((TablesToDeleteFrom & 2) > 0)
                {
                    try
                    {
                        sQuery = "DELETE FROM aspnet_UsersInRoles WHERE @UserId = UserId";
                        cmd = new MySqlCommand(sQuery, holder.Connection);
                        cmd.Transaction = sqlTrans;

                        cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                        if (cmd.ExecuteNonQuery() > 0)
                        {
                            NumTablesDeletedFrom++;
                        }
                    }
                    catch
                    {

                    }
                }

                //Delete from aspnet_Profile table if (@TablesToDeleteFrom & 4) is set
                if ((TablesToDeleteFrom & 4) > 0)
                {
                    try
                    {
                        sQuery = "DELETE FROM aspnet_Profile WHERE @UserId = UserId";
                        cmd = new MySqlCommand(sQuery, holder.Connection);
                        cmd.Transaction = sqlTrans;

                        cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                        if (cmd.ExecuteNonQuery() > 0)
                        {
                            NumTablesDeletedFrom++;
                        }
                    }
                    catch
                    {

                    }
                }

                //Delete from aspnet_PersonalizationPerUser table if (@TablesToDeleteFrom & 8) is set
                if ((TablesToDeleteFrom & 8) > 0)
                {
                    try
                    {
                        sQuery = "DELETE FROM aspnet_PersonalizationPerUser WHERE @UserId = UserId";
                        cmd = new MySqlCommand(sQuery, holder.Connection);
                        cmd.Transaction = sqlTrans;

                        cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                        if (cmd.ExecuteNonQuery() > 0)
                        {
                            NumTablesDeletedFrom++;
                        }
                    }
                    catch
                    {

                    }
                }

                //Delete from aspnet_Users table if (@TablesToDeleteFrom & 1,2,4 & 8) are all set
                if ((TablesToDeleteFrom & 1) > 0 &&
                    (TablesToDeleteFrom & 2) > 0 &&
                    (TablesToDeleteFrom & 4) > 0 &&
                    (TablesToDeleteFrom & 8) > 0)
                {
                    try
                    {
                        sQuery = "DELETE FROM aspnet_Users WHERE @UserId = UserId";
                        cmd = new MySqlCommand(sQuery, holder.Connection);
                        cmd.Transaction = sqlTrans;

                        cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                        if (cmd.ExecuteNonQuery() > 0)
                        {
                            NumTablesDeletedFrom++;
                        }
                    }
                    catch
                    {

                    }
                }
                sqlTrans.Commit();
            }
            catch 
            {
                try
                {
                    sqlTrans.Rollback();
                    return -1;
                }
                catch 
                {
                    throw;
                }
            }
            return 0;
        }

        internal static int aspnet_Membership_GetAllUsers(String ApplicationName, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty; 

            String sQuery = "CREATE TEMPORARY TABLE IF NOT EXISTS PageIndexForUsers (" +
                "IndexId int AUTO_INCREMENT NOT NULL PRIMARY KEY, " +
                "UserId binary(255) not null)";

            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.ExecuteNonQuery();

            sQuery = "TRUNCATE TABLE PageIndexForUsers";
            cmd = new MySqlCommand(sQuery, holder.Connection);
            cmd.ExecuteNonQuery();

            sQuery = "SELECT ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";

            cmd = new MySqlCommand(sQuery, holder.Connection);
            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            sQuery = "INSERT INTO PageIndexForUsers (UserId) (" +
                "SELECT aspnet_Users.UserId " +
                "FROM   aspnet_Membership, aspnet_Users " +
                "WHERE  aspnet_Users.ApplicationId = @ApplicationId AND aspnet_Users.UserId = aspnet_Membership.UserId " +
                "ORDER BY aspnet_Users.UserName)";

            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
            
            int rowcount = cmd.ExecuteNonQuery();

            return rowcount;
        }

        internal static int aspnet_Membership_FindUsersByName(String ApplicationName, String UserNameToMatch,
                SqlConnectionHolder holder) 
        {
            Guid ApplicationId = Guid.Empty;

            String sQuery = "CREATE TEMPORARY TABLE IF NOT EXISTS PageIndexForUsers (" +
                "IndexId int AUTO_INCREMENT NOT NULL PRIMARY KEY, " +
                "UserId binary(255) not null)";

            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.ExecuteNonQuery();

            sQuery = "TRUNCATE TABLE PageIndexForUsers";
            cmd = new MySqlCommand(sQuery, holder.Connection);
            cmd.ExecuteNonQuery();

            sQuery = "SELECT ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";

            cmd = new MySqlCommand(sQuery, holder.Connection);
            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            
            sQuery = "INSERT INTO PageIndexForUsers (UserId) (" +
                "SELECT aspnet_Users.UserId " +
                "FROM   aspnet_Membership, aspnet_Users " +
                "WHERE  aspnet_Users.ApplicationId = @ApplicationId AND aspnet_Membership.UserId = aspnet_Users.UserId AND aspnet_Users.LoweredUserName LIKE @UserNameToMatch" +
                "ORDER BY aspnet_Users.UserName)";

            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
            cmd.Parameters.Add(CreateInputParam("@UserNameToMatch", MySqlDbType.VarChar, UserNameToMatch.ToLower()));
            
            int rowcount = cmd.ExecuteNonQuery();

            return rowcount;
        }

        internal static int aspnet_Membership_FindUsersByEmail(String ApplicationName, String EmailToMatch,
                SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;

            String sQuery = "CREATE TEMPORARY TABLE IF NOT EXISTS PageIndexForUsers (" +
                "IndexId int AUTO_INCREMENT NOT NULL PRIMARY KEY, " +
                "UserId binary(255) not null)";

            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.ExecuteNonQuery();

            sQuery = "TRUNCATE TABLE PageIndexForUsers";
            cmd = new MySqlCommand(sQuery, holder.Connection);
            cmd.ExecuteNonQuery();

            sQuery = "SELECT ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";

            cmd = new MySqlCommand(sQuery, holder.Connection);
            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            
            if (EmailToMatch == null) 
            {
                sQuery = "INSERT INTO PageIndexForUsers (UserId) (" +
                    "SELECT aspnet_Users.UserId " +
                    "FROM   aspnet_Membership, aspnet_Users " +
                    "WHERE  aspnet_Users.ApplicationId = @ApplicationId AND aspnet_Membership.UserId = aspnet_Users.UserId AND aspnet_Membership.Email IS NULL " +
                    "ORDER BY aspnet_Membership.LoweredEmail";

                cmd = new MySqlCommand(sQuery, holder.Connection);
                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            }
            else
            {
                 sQuery = "INSERT INTO PageIndexForUsers (UserId) (" +
                    "SELECT aspnet_Users.UserId " +
                    "FROM   aspnet_Membership, aspnet_Users " +
                    "WHERE  aspnet_Users.ApplicationId = @ApplicationId AND aspnet_Membership.UserId = aspnet_Users.UserId AND aspnet_Membership.LoweredEmail LIKE @EmailToMatch " +
                    "ORDER BY aspnet_Membership.LoweredEmail";

                 cmd = new MySqlCommand(sQuery, holder.Connection);
                 cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
                 cmd.Parameters.Add(CreateInputParam("@EmailToMatch", MySqlDbType.VarChar, EmailToMatch.ToLower()));
            }

            int rowcount = cmd.ExecuteNonQuery();

            return rowcount;
        }

        internal static int aspnet_Membership_UpdateUserInfo(String ApplicationName, String UserName,
                Boolean IsPasswordCorrect, Boolean UpdateLastLoginActivityDate, int MaxInvalidPasswordAttempts,
                int PasswordAttemptWindow, DateTime CurrentTimeUtc, DateTime LastLoginDate,
                DateTime LastActivityDate, SqlConnectionHolder holder)
        {
            Guid UserId = Guid.Empty;
            Boolean IsApproved = true;
            Boolean IsLockedOut = false;
            DateTime LastLockoutDate = new DateTime(1754, 1, 1, 0, 0, 0);;
            int FailedPasswordAttemptCount = 0;
            DateTime FailedPasswordAttemptWindowStart = new DateTime(1754, 1, 1, 0, 0, 0); ;
            int FailedPasswordAnswerAttemptCount = 0;
            DateTime FailedPasswordAnswerAttemptWindowStart = new DateTime(1754, 1, 1, 0, 0, 0); ;

            MySqlTransaction sqlTrans = null;
            
            String sQuery = "SELECT  aspnet_Users.UserId, " +
                    "aspnet_Membership.IsApproved, " +
                    "aspnet_Membership.IsLockedOut, " +
                    "aspnet_Membership.LastLockoutDate, " +
                    "aspnet_Membership.FailedPasswordAttemptCount, " +
                    "aspnet_Membership.FailedPasswordAttemptWindowStart, " +
                    "aspnet_Membership.FailedPasswordAnswerAttemptCount, " +
                    "aspnet_Membership.FailedPasswordAnswerAttemptWindowStart " +
                    "FROM    aspnet_Applications, aspnet_Users, aspnet_Membership " +
                    "WHERE   @ApplicationName = aspnet_Applications.LoweredApplicationName AND " +
                    "aspnet_Users.ApplicationId = aspnet_Applications.ApplicationId    AND " +
                    "aspnet_Users.UserId = aspnet_Membership.UserId AND " +
                    "@UserName = aspnet_Users.LoweredUserName";

            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);
            
            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            
            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                    IsApproved = reader.GetBoolean(1);
                    IsLockedOut = reader.GetBoolean(2);
                    LastLockoutDate = reader.GetDateTime(3);
                    FailedPasswordAttemptCount = reader.GetInt32(4);
                    FailedPasswordAttemptWindowStart = reader.GetDateTime(5);
                    FailedPasswordAnswerAttemptCount = reader.GetInt32(6);
                    FailedPasswordAnswerAttemptWindowStart = reader.GetDateTime(7);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            
            if (UserId.Equals(Guid.Empty))
            {
                return 1;
            }

            if (IsLockedOut == true)
            {
                return 0;
            }

            if (IsPasswordCorrect == true)
            {
                if (CurrentTimeUtc > FailedPasswordAttemptWindowStart.AddMinutes(PasswordAttemptWindow))
                {
                    FailedPasswordAttemptWindowStart = CurrentTimeUtc;
                    FailedPasswordAttemptCount = 1;
                }
                else
                {
                    FailedPasswordAttemptWindowStart = CurrentTimeUtc;
                    FailedPasswordAttemptCount++;
                }

                if (FailedPasswordAttemptCount >= MaxInvalidPasswordAttempts)
                {
                    IsLockedOut = true;
                    LastLockoutDate = CurrentTimeUtc;
                }
            }
            else
            {
                if (FailedPasswordAttemptCount > 0 || FailedPasswordAnswerAttemptCount > 0)
                {
                    FailedPasswordAttemptCount = 0;
                    FailedPasswordAttemptWindowStart = new DateTime(1754, 1, 1, 0, 0, 0);
                    FailedPasswordAnswerAttemptCount = 0;
                    FailedPasswordAnswerAttemptWindowStart = new DateTime(1754, 1, 1, 0, 0, 0);
                    LastLockoutDate = new DateTime(1754, 1, 1, 0, 0, 0);
                }
            }

            try
            {
                sqlTrans = holder.Connection.BeginTransaction();
            
                if (UpdateLastLoginActivityDate == true)
                {
                    try
                    {
                        sQuery = "UPDATE  aspnet_Users SET LastActivityDate = @LastActivityDate WHERE @UserId = UserId";
                        cmd = new MySqlCommand(sQuery, holder.Connection);
                        cmd.Transaction = sqlTrans;

                        cmd.Parameters.Add(CreateInputParam("@LastActivityDate", MySqlDbType.DateTime, LastActivityDate));
                        cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
                    
                        cmd.ExecuteNonQuery();

                        sQuery = "UPDATE  aspnet_Membership SET LastLoginDate = @LastLoginDate WHERE @UserId = UserId";
                        cmd = new MySqlCommand(sQuery, holder.Connection);
                        cmd.Transaction = sqlTrans;

                        cmd.Parameters.Add(CreateInputParam("@LastLoginDate", MySqlDbType.DateTime, LastLoginDate));
                        cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                        cmd.ExecuteNonQuery();
                    }
                    catch
                    {
                        return -1;
                    }
                }
            
                sQuery = "UPDATE aspnet_Membership SET IsLockedOut = @IsLockedOut, " +
                        "LastLockoutDate = @LastLockoutDate, FailedPasswordAttemptCount = " +
                        "@FailedPasswordAttemptCount, FailedPasswordAttemptWindowStart = " +
                        "@FailedPasswordAttemptWindowStart, FailedPasswordAnswerAttemptCount = " +
                        "@FailedPasswordAnswerAttemptCount, FailedPasswordAnswerAttemptWindowStart = " +
                        "@FailedPasswordAnswerAttemptWindowStart WHERE @UserId = UserId";

                cmd = new MySqlCommand(sQuery, holder.Connection);
                cmd.Transaction = sqlTrans;

                cmd.Parameters.Add(CreateInputParam("@IsLockedOut", MySqlDbType.Bit, IsLockedOut));
                cmd.Parameters.Add(CreateInputParam("@LastLockoutDate", MySqlDbType.DateTime, LastLockoutDate));
                cmd.Parameters.Add(CreateInputParam("@FailedPasswordAttemptCount", MySqlDbType.Int32, FailedPasswordAttemptCount));
                cmd.Parameters.Add(CreateInputParam("@FailedPasswordAttemptWindowStart", MySqlDbType.DateTime, FailedPasswordAttemptWindowStart));
                cmd.Parameters.Add(CreateInputParam("@FailedPasswordAnswerAttemptCount", MySqlDbType.Int32, FailedPasswordAnswerAttemptCount));
                cmd.Parameters.Add(CreateInputParam("@FailedPasswordAnswerAttemptWindowStart", MySqlDbType.DateTime, FailedPasswordAnswerAttemptWindowStart));
                cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                cmd.ExecuteNonQuery();
                sqlTrans.Commit();
            }
            catch 
            {
                try
                {
                    sqlTrans.Rollback();
                    return -1;
                }
                catch 
                {
                    throw;
                }
            }
            return 0;
        }

        internal static int aspnet_Membership_GetPasswordWithFormat(String ApplicationName, String UserName,
                Boolean UpdateLastLoginActivityDate, DateTime CurrentTimeUtc, SqlConnectionHolder holder,
                ref MySqlCommand outcmd)
        {

            outcmd = new MySqlCommand("SELECT * FROM aspnet_Membership WHERE 1=0", holder.Connection);

            Boolean IsLockedOut = false;
            Guid UserId = Guid.Empty;
            String Password = "";
            String PasswordSalt = "";
            int PasswordFormat = 0;
            int FailedPasswordAttemptCount = 0; 
            int FailedPasswordAnswerAttemptCount = 0;
            Boolean IsApproved = true;
            DateTime LastActivityDate = new DateTime(1754, 1, 1, 0, 0, 0); ;
            DateTime LastLoginDate = new DateTime(1754, 1, 1, 0, 0, 0); ;

            String sQuery = "SELECT  aspnet_Users.UserId, " +
                "aspnet_Membership.IsLockedOut, " +    
                "Password, " +
                "PasswordFormat, " +
                "PasswordSalt, " +
                "aspnet_Membership.FailedPasswordAttemptCount, " +
                "aspnet_Membership.FailedPasswordAnswerAttemptCount, " +
                "aspnet_Membership.IsApproved, " +
                "LastActivityDate, " +
                "LastLoginDate " + 
                "FROM    aspnet_Applications, aspnet_Users, aspnet_Membership " +
                "WHERE   @ApplicationName = aspnet_Applications.LoweredApplicationName AND " +
                "aspnet_Users.ApplicationId = aspnet_Applications.ApplicationId    AND " +
                "aspnet_Users.UserId = aspnet_Membership.UserId AND " +
                "@UserName = aspnet_Users.LoweredUserName";
            
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                    IsLockedOut = reader.GetBoolean(1);
                    Password = reader.GetString(2);
                    PasswordFormat = reader.GetInt32(3);
                    PasswordSalt = reader.GetString(4);
                    FailedPasswordAttemptCount = reader.GetInt32(5);
                    FailedPasswordAnswerAttemptCount = reader.GetInt32(6);
                    IsApproved = reader.GetBoolean(7);
                    LastActivityDate = reader.GetDateTime(8);
                    LastLoginDate = reader.GetDateTime(9);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            
            if (UserId.Equals(Guid.Empty))
            {
                return 1;
            }

            if (IsLockedOut == true) 
            {
                return 99;
            }
            if (UpdateLastLoginActivityDate == true && IsApproved == true)
            {
                sQuery = "UPDATE  aspnet_Users SET LastActivityDate = @LastActivityDate WHERE @UserId = UserId";
                cmd = new MySqlCommand(sQuery, holder.Connection);
                        
                cmd.Parameters.Add(CreateInputParam("@LastActivityDate", MySqlDbType.DateTime, CurrentTimeUtc));
                cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
                    
                cmd.ExecuteNonQuery();

                sQuery = "UPDATE  aspnet_Membership SET LastLoginDate = @LastLoginDate WHERE @UserId = UserId";
                cmd = new MySqlCommand(sQuery, holder.Connection);
                        
                cmd.Parameters.Add(CreateInputParam("@LastLoginDate", MySqlDbType.DateTime, CurrentTimeUtc));
                cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                cmd.ExecuteNonQuery();

                LastActivityDate = CurrentTimeUtc;
                LastLoginDate = CurrentTimeUtc;
            }

            sQuery = "SELECT   @Password, @PasswordFormat, @PasswordSalt, @FailedPasswordAttemptCount, " +
                "@FailedPasswordAnswerAttemptCount, @IsApproved, CAST(@LastLoginDate AS DATETIME), CAST(@LastActivityDate AS DATETIME)";
            outcmd = new MySqlCommand(sQuery, holder.Connection);

            outcmd.Parameters.Add(CreateInputParam("@Password", MySqlDbType.VarChar, Password));
            outcmd.Parameters.Add(CreateInputParam("@PasswordFormat", MySqlDbType.Int32, PasswordFormat));
            outcmd.Parameters.Add(CreateInputParam("@PasswordSalt", MySqlDbType.VarChar, PasswordSalt));
            outcmd.Parameters.Add(CreateInputParam("@FailedPasswordAttemptCount", MySqlDbType.Int32, FailedPasswordAttemptCount));
            outcmd.Parameters.Add(CreateInputParam("@FailedPasswordAnswerAttemptCount", MySqlDbType.Int32, FailedPasswordAnswerAttemptCount));
            outcmd.Parameters.Add(CreateInputParam("@IsApproved", MySqlDbType.Bit, IsApproved));
            outcmd.Parameters.Add(CreateInputParam("@LastLoginDate", MySqlDbType.DateTime, LastLoginDate));
            outcmd.Parameters.Add(CreateInputParam("@LastActivityDate", MySqlDbType.DateTime, LastActivityDate));

            return 0;
        }


        internal static int aspnet_Membership_GetPassword(String ApplicationName, String UserName,
                int MaxInvalidPasswordAttempts, int PasswordAttemptWindow, DateTime CurrentTimeUtc,
                String PasswordAnswer, SqlConnectionHolder holder, ref MySqlCommand outcmd)
        {
            outcmd = new MySqlCommand("SELECT * FROM aspnet_Membership WHERE 1=0", holder.Connection);

            Guid UserId = Guid.Empty;
            int PasswordFormat = 0;
            String Password = "";
            String passAns = "";
            Boolean IsLockedOut = false;
            DateTime LastLockoutDate = new DateTime(1754, 1, 1, 0, 0, 0); ;
            int FailedPasswordAttemptCount = 0;
            DateTime FailedPasswordAttemptWindowStart = new DateTime(1754, 1, 1, 0, 0, 0); ;
            int FailedPasswordAnswerAttemptCount = 0;
            DateTime FailedPasswordAnswerAttemptWindowStart = new DateTime(1754, 1, 1, 0, 0, 0); ;

            MySqlTransaction sqlTrans = null;

            String sQuery = "SELECT aspnet_Users.UserId, " +
                "aspnet_Membership.Password, " +
                "aspnet_Membership.PasswordAnswer, " +
                "aspnet_Membership.PasswordFormat, " +
                "aspnet_Membership.IsLockedOut, " +
                "aspnet_Membership.LastLockoutDate, " +
                "aspnet_Membership.FailedPasswordAttemptCount, " +
                "aspnet_Membership.FailedPasswordAttemptWindowStart, " +
                "aspnet_Membership.FailedPasswordAnswerAttemptCount, " +
                "aspnet_Membership.FailedPasswordAnswerAttemptWindowStart " +
                "FROM aspnet_Applications, aspnet_Users, aspnet_Membership " +
                "WHERE   @ApplicationName = aspnet_Applications.LoweredApplicationName AND " +
                "aspnet_Users.ApplicationId = aspnet_Applications.ApplicationId    AND " +
                "aspnet_Users.UserId = aspnet_Membership.UserId AND " +
                "@UserName = aspnet_Users.LoweredUserName";

            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            
            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                    Password = reader.GetString(1);
                    passAns = reader.GetString(2);
                    PasswordFormat = reader.GetInt32(3);
                    IsLockedOut = reader.GetBoolean(4);
                    LastLockoutDate = reader.GetDateTime(5);
                    FailedPasswordAttemptCount = reader.GetInt32(6);
                    FailedPasswordAttemptWindowStart = reader.GetDateTime(7);
                    FailedPasswordAnswerAttemptCount = reader.GetInt32(8);
                    FailedPasswordAnswerAttemptWindowStart = reader.GetDateTime(9);

                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (UserId.Equals(Guid.Empty))
            {
                return 1;
            }

            if (!String.IsNullOrEmpty(PasswordAnswer)) 
            {
                if (!String.IsNullOrEmpty(passAns) || passAns.ToLower() != PasswordAnswer.ToLower())
                {
                    if (CurrentTimeUtc > FailedPasswordAnswerAttemptWindowStart.AddMinutes(PasswordAttemptWindow)) 
                    {
                        FailedPasswordAnswerAttemptWindowStart = CurrentTimeUtc;
                        FailedPasswordAnswerAttemptCount = 1;
                    }
                    else
                    {
                        FailedPasswordAnswerAttemptWindowStart = CurrentTimeUtc;
                        FailedPasswordAnswerAttemptCount++;
                    }
                    if (FailedPasswordAnswerAttemptCount >= MaxInvalidPasswordAttempts) 
                    {
                        IsLockedOut = true;
                        LastLockoutDate = CurrentTimeUtc;
                    }
                    return 3;
                }
                else
                {
                    if (FailedPasswordAnswerAttemptCount > 0) 
                    {
                        FailedPasswordAnswerAttemptCount = 0;
                        FailedPasswordAnswerAttemptWindowStart = new DateTime(1754, 1, 1, 0, 0, 0);
                    }
                }

                try
                {
                    sqlTrans = holder.Connection.BeginTransaction();
                    sQuery = "UPDATE aspnet_Membership " +
                        "SET IsLockedOut = @IsLockedOut, LastLockoutDate = @LastLockoutDate, " +
                        "FailedPasswordAttemptCount = @FailedPasswordAttemptCount, " +
                        "FailedPasswordAttemptWindowStart = @FailedPasswordAttemptWindowStart, " +
                        "FailedPasswordAnswerAttemptCount = @FailedPasswordAnswerAttemptCount, " +
                        "FailedPasswordAnswerAttemptWindowStart = @FailedPasswordAnswerAttemptWindowStart " +
                        "WHERE @UserId = UserId";

                    cmd = new MySqlCommand(sQuery, holder.Connection);
                    cmd.Transaction = sqlTrans;

                    cmd.Parameters.Add(CreateInputParam("@IsLockedOut", MySqlDbType.Bit, IsLockedOut));
                    cmd.Parameters.Add(CreateInputParam("@LastLockoutDate", MySqlDbType.DateTime, LastLockoutDate));
                    cmd.Parameters.Add(CreateInputParam("@FailedPasswordAttemptCount", MySqlDbType.Int32, FailedPasswordAttemptCount));
                    cmd.Parameters.Add(CreateInputParam("@FailedPasswordAttemptWindowStart", MySqlDbType.DateTime, FailedPasswordAttemptWindowStart));
                    cmd.Parameters.Add(CreateInputParam("@FailedPasswordAnswerAttemptCount", MySqlDbType.Int32, FailedPasswordAnswerAttemptCount));
                    cmd.Parameters.Add(CreateInputParam("@FailedPasswordAnswerAttemptWindowStart", MySqlDbType.DateTime, FailedPasswordAnswerAttemptWindowStart));
                    cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                    cmd.ExecuteNonQuery();
                    sqlTrans.Commit(); 
                }
                catch 
                {
                    try
                    {
                        sqlTrans.Rollback();
                        return -1;
                    }
                    catch 
                    {
                        throw;
                    }
                }
            }

            sQuery = "SELECT @Password, @PasswordFormat";

            outcmd = new MySqlCommand(sQuery, holder.Connection);

            outcmd.Parameters.Add(CreateInputParam("@Password", MySqlDbType.VarChar, Password));
            outcmd.Parameters.Add(CreateInputParam("@PasswordFormat", MySqlDbType.Int32, PasswordFormat));

            return 0;
        }


        internal static int aspnet_UsersInRoles_IsUserInRole(String ApplicationName, String UserName, String RoleName,
                SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;
            Guid UserId = Guid.Empty;
            Guid RoleId = Guid.Empty;

            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));
 
            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 2;
            }

            sQuery = "SELECT  UserId FROM aspnet_Users WHERE LoweredUserName = @UserName " +
                    "AND ApplicationId = @ApplicationId";
            cmd = new MySqlCommand(sQuery, holder.Connection);
            
            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
 
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (UserId.Equals(Guid.Empty))
            {
                return 2;
            }

            sQuery = "SELECT RoleId FROM aspnet_Roles WHERE LoweredRoleName = @RoleName AND " +
                    "ApplicationId = @ApplicationId";
            cmd = new MySqlCommand(sQuery, holder.Connection);
            
            cmd.Parameters.Add(CreateInputParam("@RoleName", MySqlDbType.VarChar, RoleName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
 
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    RoleId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (RoleId.Equals(Guid.Empty))
            {
                return 3;
            }

            sQuery = "SELECT COUNT(UserId) FROM aspnet_UsersInRoles WHERE UserId = @UserId AND RoleId = @RoleId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
            cmd.Parameters.Add(CreateInputParam("@RoleId", MySqlDbType.Binary, RoleId));

            if ((long)cmd.ExecuteScalar() > 0)
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }

        internal static int aspnet_UsersInRoles_GetRolesForUser(String ApplicationName, String UserName, 
                SqlConnectionHolder holder, ref MySqlCommand outcmd)
        {
            String sQuery = "SELECT * FROM aspnet_Roles WHERE 1=0";

            outcmd = new MySqlCommand(sQuery, holder.Connection);
            
            Guid ApplicationId = Guid.Empty;
            Guid UserId = Guid.Empty;

            sQuery = "SELECT ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 1;
            }

            sQuery = "SELECT UserId FROM aspnet_Users WHERE LoweredUserName = @UserName AND ApplicationId = @ApplicationId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            if (UserId.Equals(Guid.Empty))
            {
                return 1;
            }

            sQuery = "SELECT aspnet_Roles.RoleName " +
                "FROM   aspnet_Roles, aspnet_UsersInRoles " +
                "WHERE  aspnet_Roles.RoleId = aspnet_UsersInRoles.RoleId AND aspnet_Roles.ApplicationId = " +
                "@ApplicationId AND aspnet_UsersInRoles.UserId = @UserId " +
                "ORDER BY aspnet_Roles.RoleName";
            
            outcmd = new MySqlCommand(sQuery, holder.Connection);

            outcmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
            outcmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            return 0;
        }

        internal static int aspnet_Roles_CreateRole(String ApplicationName, String RoleName, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;
            MySqlTransaction sqlTrans = null;

            ApplicationId = aspnet_Applications_CreateApplication(holder, ApplicationName);

            String sQuery = "SELECT COUNT(RoleId) FROM aspnet_Roles WHERE LoweredRoleName = @RoleName AND ApplicationId = @ApplicationId";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@RoleName", MySqlDbType.VarChar, RoleName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            if ((long)cmd.ExecuteScalar() > 0)
            {
                return 1;
            }

            try
            {
                sqlTrans = holder.Connection.BeginTransaction();

                Guid RoleId = Guid.NewGuid();

                sQuery = "INSERT INTO aspnet_Roles (ApplicationId, RoleId, RoleName, LoweredRoleName) " +
                        "VALUES (@ApplicationId, @RoleId, @RoleName, @LoweredRoleName)";
                cmd = new MySqlCommand(sQuery, holder.Connection);
                cmd.Transaction = sqlTrans;

                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
                cmd.Parameters.Add(CreateInputParam("@RoleId", MySqlDbType.Binary, RoleId));
                cmd.Parameters.Add(CreateInputParam("@RoleName", MySqlDbType.VarChar, RoleName));
                cmd.Parameters.Add(CreateInputParam("@LoweredRoleName", MySqlDbType.VarChar, RoleName.ToLower()));

                cmd.ExecuteNonQuery();
                sqlTrans.Commit();
            }
            catch
            {
                try
                {
                    sqlTrans.Rollback();
                    return -1;
                }
                catch 
                {
                    throw;
                }
            }
            
            return 0;
        }

        internal static int aspnet_Roles_DeleteRole(String ApplicationName, String RoleName, 
                    Boolean DeleteOnlyIfRoleIsEmpty, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;
            Guid RoleId = Guid.Empty;
            MySqlTransaction sqlTrans = null;

            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 1;
            }

            sQuery = "SELECT RoleId FROM aspnet_Roles WHERE LoweredRoleName = @RoleName AND " +
                    "ApplicationId = @ApplicationId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@RoleName", MySqlDbType.VarChar, RoleName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    RoleId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (RoleId.Equals(Guid.Empty))
            {
                return 1;
            }

            if (DeleteOnlyIfRoleIsEmpty == true)
            {
                sQuery = "SELECT Count(RoleId) FROM aspnet_UsersInRoles WHERE @RoleId = RoleId";

                cmd = new MySqlCommand(sQuery, holder.Connection);

                cmd.Parameters.Add(CreateInputParam("@RoleId", MySqlDbType.Binary, RoleId));

                if ((long)cmd.ExecuteScalar() > 0)
                {
                    return 2;
                }
            }

            try
            {
                sqlTrans = holder.Connection.BeginTransaction();

                sQuery = "DELETE FROM aspnet_UsersInRoles  WHERE @RoleId = RoleId";
                cmd = new MySqlCommand(sQuery, holder.Connection);
                cmd.Transaction = sqlTrans;

                cmd.Parameters.Add(CreateInputParam("@RoleId", MySqlDbType.Binary, RoleId));

                cmd.ExecuteNonQuery();

                sQuery = "DELETE FROM aspnet_Roles WHERE @RoleId = RoleId  AND ApplicationId = @ApplicationId";
                cmd = new MySqlCommand(sQuery, holder.Connection);
                cmd.Transaction = sqlTrans;

                cmd.Parameters.Add(CreateInputParam("@RoleId", MySqlDbType.Binary, RoleId));
                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

                cmd.ExecuteNonQuery();

                sqlTrans.Commit();
            }
            catch 
            {
                try
                {
                    sqlTrans.Rollback();
                    return -1;
                }
                catch
                {
                    throw;
                }
            }
            return 0;
        }

        internal static int aspnet_Roles_RoleExists(String ApplicationName, String RoleName, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;

            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 0;
            }

            sQuery = "SELECT COUNT(RoleName) FROM aspnet_Roles WHERE @RoleName = LoweredRoleName AND " +
                "ApplicationId = @ApplicationId";

            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@RoleName", MySqlDbType.VarChar, RoleName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            if ((long)cmd.ExecuteScalar() > 0)
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }

        internal static int aspnet_UsersInRoles_AddUsersToRoles(String ApplicationName, String[] UserNames,
                String[] RoleNames, DateTime CurrentTimeUtc, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;
            
            MySqlTransaction sqlTrans = null;

            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 2;
            }

            sqlTrans = holder.Connection.BeginTransaction();
            try
            {
                foreach (String u in UserNames)
                {
                    Guid UserId = Guid.Empty;

                    sQuery = "SELECT UserId FROM aspnet_Users WHERE LoweredUserName = @UserName AND ApplicationId = @ApplicationId";
                    cmd = new MySqlCommand(sQuery, holder.Connection);

                    cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, u.ToLower()));
                    cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

                    reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

                    try
                    {
                        if (reader.Read())
                        {
                            UserId = reader.GetGuid(0);
                        }
                    }
                    catch
                    {

                    }
                    finally
                    {
                        if (reader != null)
                        {
                            reader.Close();
                            reader = null;
                        }
                    }
                    if (UserId.Equals(Guid.Empty))
                    {
                        return 1;
                    }
                    
                    foreach (String r in RoleNames)
                    {
                        Guid RoleId = Guid.Empty;

                        sQuery = "SELECT RoleId FROM aspnet_Roles WHERE LoweredRoleName = @RoleName AND " +
                                "ApplicationId = @ApplicationId";
                        cmd = new MySqlCommand(sQuery, holder.Connection);

                        cmd.Parameters.Add(CreateInputParam("@RoleName", MySqlDbType.VarChar, r.ToLower()));
                        cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

                        reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

                        try
                        {
                            if (reader.Read())
                            {
                                RoleId = reader.GetGuid(0);
                            }
                        }
                        catch
                        {

                        }
                        finally
                        {
                            if (reader != null)
                            {
                                reader.Close();
                                reader = null;
                            }
                        }

                        if (RoleId.Equals(Guid.Empty))
                        {
                            return 2;
                        }

                        sQuery = "SELECT COUNT(UserId) FROM aspnet_UsersInRoles WHERE @UserId = UserId AND @RoleId = RoleId";
                        cmd = new MySqlCommand(sQuery, holder.Connection);

                        cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
                        cmd.Parameters.Add(CreateInputParam("@RoleId", MySqlDbType.Binary, RoleId));

                        if ((long)cmd.ExecuteScalar() > 0)
                        {
                            return 3;
                        }
                        
                        sQuery = "INSERT INTO aspnet_UsersInRoles (UserId, RoleId) VALUES (@UserId, @RoleId)";
                        cmd = new MySqlCommand(sQuery, holder.Connection);
                        cmd.Transaction = sqlTrans;

                        cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
                        cmd.Parameters.Add(CreateInputParam("@RoleId", MySqlDbType.Binary, RoleId));

                        cmd.ExecuteNonQuery();
                    }
                }
                sqlTrans.Commit();
            }
            catch
            {
                try
                {
                    sqlTrans.Rollback();
                    return -1;
                }
                catch
                {
                    throw;
                }
            }
            return 0;
        }

        internal static int aspnet_UsersInRoles_RemoveUsersFromRoles(String ApplicationName, String[] UserNames,
                String[] RoleNames, DateTime CurrentTimeUtc, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;

            MySqlTransaction sqlTrans = null;

            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 2;
            }

            sqlTrans = holder.Connection.BeginTransaction();
            try
            {
                foreach (String u in UserNames)
                {
                    Guid UserId = Guid.Empty;

                    sQuery = "SELECT UserId FROM aspnet_Users WHERE LoweredUserName = @UserName AND ApplicationId = @ApplicationId";
                    cmd = new MySqlCommand(sQuery, holder.Connection);

                    cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, u.ToLower()));
                    cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

                    reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

                    try
                    {
                        if (reader.Read())
                        {
                            UserId = reader.GetGuid(0);
                        }
                    }
                    catch
                    {

                    }
                    finally
                    {
                        if (reader != null)
                        {
                            reader.Close();
                            reader = null;
                        }
                    }
                    if (UserId.Equals(Guid.Empty))
                    {
                        return 1;
                    }

                    foreach (String r in RoleNames)
                    {
                        Guid RoleId = Guid.Empty;

                        sQuery = "SELECT RoleId FROM aspnet_Roles WHERE LoweredRoleName = @RoleName AND " +
                                "ApplicationId = @ApplicationId";
                        cmd = new MySqlCommand(sQuery, holder.Connection);

                        cmd.Parameters.Add(CreateInputParam("@RoleName", MySqlDbType.VarChar, r.ToLower()));
                        cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

                        reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

                        try
                        {
                            if (reader.Read())
                            {
                                RoleId = reader.GetGuid(0);
                            }
                        }
                        catch
                        {

                        }
                        finally
                        {
                            if (reader != null)
                            {
                                reader.Close();
                                reader = null;
                            }
                        }

                        if (RoleId.Equals(Guid.Empty))
                        {
                            return 2;
                        }

                        sQuery = "SELECT COUNT(UserId) FROM aspnet_UsersInRoles WHERE @UserId = UserId AND @RoleId = RoleId";
                        cmd = new MySqlCommand(sQuery, holder.Connection);

                        cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
                        cmd.Parameters.Add(CreateInputParam("@RoleId", MySqlDbType.Binary, RoleId));

                        if ((long)cmd.ExecuteScalar() <= 0)
                        {
                            return 3;
                        }

                        sQuery = "DELETE FROM aspnet_UsersInRoles WHERE @UserId = UserId AND @RoleId = RoleId";
                        cmd = new MySqlCommand(sQuery, holder.Connection);
                        cmd.Transaction = sqlTrans;

                        cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
                        cmd.Parameters.Add(CreateInputParam("@RoleId", MySqlDbType.Binary, RoleId));

                        cmd.ExecuteNonQuery();
                    }
                }
                sqlTrans.Commit();
            }
            catch
            {
                try
                {
                    sqlTrans.Rollback();
                    return -1;
                }
                catch
                {
                    throw;
                }
            }
            return 0;
        }

        internal static int aspnet_UsersInRoles_GetUsersInRoles(String ApplicationName, String RoleName, 
                SqlConnectionHolder holder, ref MySqlCommand outcmd)
        {
            String sQuery = "SELECT * FROM aspnet_Roles WHERE 1=0";
            outcmd = new MySqlCommand(sQuery, holder.Connection);
            
            Guid ApplicationId = Guid.Empty;
            Guid RoleId = Guid.Empty;

            sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 1;
            }

            sQuery = "SELECT RoleId FROM aspnet_Roles WHERE LoweredRoleName = @RoleName AND " +
                                        "ApplicationId = @ApplicationId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@RoleName", MySqlDbType.VarChar, RoleName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    RoleId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (RoleId.Equals(Guid.Empty))
            {
                return 1;
            }

            sQuery = "SELECT aspnet_Users.UserName " +
                "FROM   aspnet_Users, aspnet_UsersInRoles " +
                "WHERE  aspnet_Users.UserId = aspnet_UsersInRoles.UserId AND @RoleId = " + 
                "aspnet_UsersInRoles.RoleId AND aspnet_Users.ApplicationId = @ApplicationId " +
                "ORDER BY aspnet_Users.UserName";

            outcmd = new MySqlCommand(sQuery, holder.Connection);

            outcmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
            outcmd.Parameters.Add(CreateInputParam("@RoleId", MySqlDbType.Binary, RoleId));
  
            return 0;
        }

        internal static int aspnet_Roles_GetAllRoles(String ApplicationName, SqlConnectionHolder holder,
            ref MySqlCommand outcmd)
        {
            String sQuery = "SELECT * FROM aspnet_Roles WHERE 1=0";
            outcmd = new MySqlCommand(sQuery, holder.Connection);

            Guid ApplicationId = Guid.Empty;

            
            sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 1;
            }

            sQuery = "SELECT RoleName FROM aspnet_Roles WHERE ApplicationId = @ApplicationId ORDER BY RoleName";

            outcmd = new MySqlCommand(sQuery, holder.Connection);

            outcmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            return 0;
        }


        internal static int aspnet_UsersInRoles_FindUsersInRole(String ApplicationName, String RoleName,
                String UserNameToMatch, SqlConnectionHolder holder, ref MySqlCommand outcmd)
        {
            String sQuery = "SELECT * FROM aspnet_Roles WHERE 1=0";
            outcmd = new MySqlCommand(sQuery, holder.Connection);

            Guid ApplicationId = Guid.Empty;
            Guid RoleId = Guid.Empty;

            sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 1;
            }

            sQuery = "SELECT RoleId FROM aspnet_Roles WHERE LoweredRoleName = @RoleName AND " +
                                "ApplicationId = @ApplicationId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@RoleName", MySqlDbType.VarChar, RoleName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    RoleId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (RoleId.Equals(Guid.Empty))
            {
                return 1;
            }

            sQuery = "SELECT aspnet_Users.UserName " + 
                "FROM  aspnet_Users, aspnet_UsersInRoles " +
                "WHERE  aspnet_Users.UserId = aspnet_UsersInRoles.UserId AND @RoleId = " +
                "aspnet_UsersInRoles.RoleId AND aspnet_Users.ApplicationId = @ApplicationId AND " +
                "LoweredUserName LIKE @UserNameToMatch " +
                "ORDER BY aspnet_Users.UserName";

            outcmd = new MySqlCommand(sQuery, holder.Connection);

            outcmd.Parameters.Add(CreateInputParam("@RoleId", MySqlDbType.Binary, RoleId));
            outcmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
            outcmd.Parameters.Add(CreateInputParam("@UserNameToMatch", MySqlDbType.VarChar, UserNameToMatch.ToLower()));

            return 0;
        }


        internal static int aspnet_Profile_GetProperties(String ApplicationName, String UserName, 
                DateTime CurrentTimeUtc, SqlConnectionHolder holder, ref MySqlCommand outcmd)
        {
            String sQuery = "SELECT UserId FROM aspnet_Users WHERE 1=0";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            Guid ApplicationId = Guid.Empty;
            Guid UserId = Guid.Empty;

            sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 1;
            }
            sQuery = "SELECT UserId FROM aspnet_Users WHERE LoweredUserName = @UserName AND ApplicationId = @ApplicationId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            if (UserId.Equals(Guid.Empty))
            {
                return 1;
            }

            sQuery = "SELECT PropertyNames, PropertyValuesString, PropertyValuesBinary " +
                "FROM         aspnet_Profile " +
                "WHERE        UserId = @UserId LIMIT 0 , 1";

            outcmd = new MySqlCommand(sQuery, holder.Connection);
            outcmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

            sQuery = "UPDATE aspnet_Users SET    LastActivityDate=@CurrentTimeUtc WHERE  UserId = @UserId";
            
            cmd = new MySqlCommand(sQuery, holder.Connection);
            
            cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", MySqlDbType.DateTime, CurrentTimeUtc));
            cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

            cmd.ExecuteNonQuery();

            return 0;
        }


        internal static int aspnet_Profile_SetProperties(String ApplicationName, String PropertyNames,
                    String PropertyValuesString, Byte[] PropertyValuesBinary, String UserName, Boolean IsUserAnonymous,
                    DateTime CurrentTimeUtc, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;
            Guid UserId = Guid.Empty;
            DateTime LastActivityDate = CurrentTimeUtc;
            MySqlTransaction sqlTrans = null;

            ApplicationId = MySqlStoredProcedures.aspnet_Applications_CreateApplication(holder, ApplicationName);

            String sQuery = "SELECT UserId FROM aspnet_Users WHERE LoweredUserName = @UserName AND ApplicationId = @ApplicationId";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            MySqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            if (UserId.Equals(Guid.Empty))
            {
                UserId = MySqlStoredProcedures.aspnet_Users_CreateUser(ApplicationId, UserName, IsUserAnonymous,
                        LastActivityDate, holder);
            }

            sQuery = "UPDATE aspnet_Users SET LastActivityDate=@CurrentTimeUtc WHERE  UserId = @UserId";

            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", MySqlDbType.DateTime, CurrentTimeUtc));
            cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

            cmd.ExecuteNonQuery();

            sQuery = "SELECT COUNT(UserId) FROM aspnet_Profile WHERE UserId = @UserId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

            if ((long)cmd.ExecuteScalar() > 0)
            {
                sQuery = "UPDATE aspnet_Profile SET PropertyNames = @PropertyNames, " +
                    "PropertyValuesString = @PropertyValuesString, " +
                    "PropertyValuesBinary = @PropertyValuesBinary, LastUpdatedDate = @CurrentTimeUtc " +
                    "WHERE  UserId = @UserId";
            }
            else
            {
                sQuery = "INSERT INTO aspnet_Profile(UserId, PropertyNames, PropertyValuesString, " +
                    "PropertyValuesBinary, LastUpdatedDate) VALUES (@UserId, @PropertyNames, " +
                    "@PropertyValuesString, @PropertyValuesBinary, @CurrentTimeUtc)";
            }

            try
            {
                sqlTrans = holder.Connection.BeginTransaction();

                cmd = new MySqlCommand(sQuery, holder.Connection);
                cmd.Transaction = sqlTrans;

                cmd.Parameters.Add(CreateInputParam("@PropertyName", MySqlDbType.Text, PropertyNames));
                cmd.Parameters.Add(CreateInputParam("@PropertyValuesString", MySqlDbType.Text, PropertyValuesString));
                cmd.Parameters.Add(CreateInputParam("@PropertyValuesBinary", MySqlDbType.Blob, PropertyValuesBinary));
                cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", MySqlDbType.DateTime, CurrentTimeUtc));
                cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                cmd.ExecuteNonQuery();
                sqlTrans.Commit();
            }
            catch
            {
                try
                {
                    sqlTrans.Rollback();
                    return -1;
                }
                catch
                {
                    throw;
                }
            }
            
            return 0;
        }

        internal static int aspnet_Profile_DeleteProfiles(String ApplicationName, String[] UserNames, 
                SqlConnectionHolder holder)
        {
            int UsersDeleted = 0;

            foreach (String u in UserNames)
            {
                int ret = 0;

                int status = MySqlStoredProcedures.aspnet_Users_DeleteUser(ApplicationName, u, 4, ref ret, holder);

                if (ret != 0)
                {
                    UsersDeleted++;
                }
            }
            return UsersDeleted;
        }

        internal static int aspnet_Profile_DeleteInactiveProfiles(String ApplicationName, int ProfileAuthOptions,
                DateTime InactiveSinceDate, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;

            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 0;
            }

            sQuery = "DELETE FROM aspnet_Profile WHERE UserId IN (SELECT UserId FROM aspnet_Users " +
                "WHERE ApplicationId = @ApplicationId AND (LastActivityDate <= @InactiveSinceDate) " +
                "AND ((@ProfileAuthOptions = 2) OR (@ProfileAuthOptions = 0 AND IsAnonymous = 1) " +
                "OR (@ProfileAuthOptions = 1 AND IsAnonymous = 0)))";

            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
            cmd.Parameters.Add(CreateInputParam("@InactiveSinceDate", MySqlDbType.DateTime, InactiveSinceDate));
            cmd.Parameters.Add(CreateInputParam("@ProfileAuthOptions", MySqlDbType.Int32, ProfileAuthOptions));

            int RowsDeleted = cmd.ExecuteNonQuery();

            return RowsDeleted;
        }

        internal static int aspnet_Profile_GetNumberOfInactiveProfiles(String ApplicationName, int ProfileAuthOptions,
                DateTime InactiveSinceDate, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;

            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 0;
            }

            sQuery = "SELECT COUNT(*) FROM aspnet_Users, aspnet_Profile WHERE ApplicationId = @ApplicationId " +
                "AND aspnet_Users.UserId = aspnet_Profile.UserId AND (LastActivityDate <= @InactiveSinceDate) AND ( " +
                "(@ProfileAuthOptions = 2) OR (@ProfileAuthOptions = 0 AND IsAnonymous = 1) " +
                "OR (@ProfileAuthOptions = 1 AND IsAnonymous = 0))";

            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
            cmd.Parameters.Add(CreateInputParam("@InactiveSinceDate", MySqlDbType.DateTime, InactiveSinceDate));
            cmd.Parameters.Add(CreateInputParam("@ProfileAuthOptions", MySqlDbType.Int32, ProfileAuthOptions));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            int NumberInactiveProfiles = 0;

            try
            {
                if (reader.Read())
                {
                    NumberInactiveProfiles = reader.GetInt32(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            return NumberInactiveProfiles;
        }

        internal static int aspnet_Profile_GetProfiles(String ApplicationName, int ProfileAuthOptions, 
                int PageIndex, int PageSize, MySqlParameter[] args, SqlConnectionHolder holder, ref MySqlCommand outcmd)
        {
            String sQuery = "SELECT * FROM aspnet_Profile WHERE 1=0";
            outcmd = new MySqlCommand(sQuery, holder.Connection);
            
            String UserNameToMatch = "";
            DateTime InactiveSinceDate = new DateTime(1754, 1, 1, 0, 0, 0);

            foreach (MySqlParameter arg in args)
            {
                if (arg.ParameterName == "@UserNameToMatch")
                {
                    UserNameToMatch = (String)arg.Value;
                }
                else if (arg.ParameterName == "InactiveSinceDate")
                {
                    InactiveSinceDate = (DateTime)arg.Value;
                }
            }
            
            Guid ApplicationId = Guid.Empty;

            sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 0;
            }

            int PageLowerBound = PageSize * PageIndex;
            int PageUpperBound = PageSize - 1 + PageLowerBound;

            sQuery = "CREATE TEMPORARY TABLE IF NOT EXISTS PageIndexForUsers (" +
                "IndexId int AUTO_INCREMENT NOT NULL PRIMARY KEY, " +
                "UserId binary(255) not null)";

            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.ExecuteNonQuery();

            sQuery = "TRUNCATE TABLE PageIndexForUsers";
            cmd = new MySqlCommand(sQuery, holder.Connection);
            cmd.ExecuteNonQuery();

            sQuery = "INSERT INTO PageIndexForUsers (UserId) ( " +
                "SELECT  aspnet_Users.UserId " +
                "FROM    aspnet_Users, aspnet_Profile " +
                "WHERE   ApplicationId = @ApplicationId " +
                "AND aspnet_Users.UserId = aspnet_Profile.UserId " +
                "AND (@InactiveSinceDate IS NULL OR LastActivityDate <= @InactiveSinceDate) " +
                "AND (     (@ProfileAuthOptions = 2) " +
                "OR (@ProfileAuthOptions = 0 AND IsAnonymous = 1) " +
                "OR (@ProfileAuthOptions = 1 AND IsAnonymous = 0) " +
                ") " +
                "AND (@UserNameToMatch IS NULL OR LoweredUserName LIKE @UserNameToMatch) " +
                "ORDER BY UserName )";

            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
            cmd.Parameters.Add(CreateInputParam("@InactiveSinceDate", MySqlDbType.DateTime, InactiveSinceDate));
            cmd.Parameters.Add(CreateInputParam("@ProfileAuthOptions", MySqlDbType.Int32, ProfileAuthOptions));
            cmd.Parameters.Add(CreateInputParam("@UserNameToMatch", MySqlDbType.VarChar, UserNameToMatch.ToLower()));

            cmd.ExecuteNonQuery();

            sQuery = "SELECT  aspnet_Users.UserName, aspnet_Users.IsAnonymous, aspnet_Users.LastActivityDate, " +
                "aspnet_Profile.LastUpdatedDate, LENGTH(aspnet_Profile.PropertyNames) + " +
                "LENGTH(aspnet_Profile.PropertyValuesString) + LENGTH(aspnet_Profile.PropertyValuesBinary) " +
                "FROM    aspnet_Users, aspnet_Profile, PageIndexForUsers WHERE aspnet_Users.UserId = " +
                "aspnet_Profile.UserId AND aspnet_Profile.UserId = PageIndexForUsers.UserId AND " +
                "PageIndexForUsers.IndexId >= @PageLowerBound AND PageIndexForUsers.IndexId <= @PageUpperBound ";

            outcmd = new MySqlCommand(sQuery, holder.Connection);

            outcmd.Parameters.Add(CreateInputParam("@PageLowerBound", MySqlDbType.Int32, PageLowerBound));
            outcmd.Parameters.Add(CreateInputParam("@PageUpperBound", MySqlDbType.Int32, PageUpperBound));

            return 0;
        }


        internal static int aspnet_PersonalizationAdministration_FindState(Boolean AllUsersScope, 
                String ApplicationName, int PageIndex, int PageSize, String Path, String UserName,
                DateTime InactiveSinceDate, SqlConnectionHolder holder, ref MySqlCommand outcmd)
        {
            String sQuery = "SELECT * FROM aspnet_Paths WHERE 1=0";
            outcmd = new MySqlCommand(sQuery, holder.Connection);

            Guid ApplicationId = Guid.Empty;

            sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 0;
            }
        
            int PageLowerBound = PageSize * PageIndex;
            int PageUpperBound = PageSize - 1 + PageLowerBound;

            sQuery = "CREATE TEMPORARY TABLE IF NOT EXISTS PageIndex (" +
                "IndexId int AUTO_INCREMENT NOT NULL PRIMARY KEY, " +
                "ItemId binary(255) not null)";

            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.ExecuteNonQuery();

            sQuery = "TRUNCATE TABLE PageIndex";
            cmd = new MySqlCommand(sQuery, holder.Connection);
            cmd.ExecuteNonQuery();

            int totalRecords = 0;

            if (AllUsersScope)
            {
                sQuery = "INSERT INTO PageIndex (ItemId) (" +
                    "SELECT aspnet_Paths.PathId " +
                    "FROM aspnet_Paths, " +
                    "((SELECT aspnet_Paths.PathId " +
                    "FROM aspnet_PersonalizationAllUsers, aspnet_Paths " +
                    "WHERE aspnet_Paths.ApplicationId = @ApplicationId " +
                    "AND aspnet_PersonalizationAllUsers.PathId = aspnet_Paths.PathId " +
                    "AND (@Path IS NULL OR aspnet_Paths.LoweredPath LIKE @Path) " +
                    ") AS SharedDataPerPath " +
                    "FULL OUTER JOIN " +
                    "(SELECT DISTINCT aspnet_Paths.PathId " +
                    "FROM aspnet_PersonalizationPerUser, aspnet_Paths " +
                    "WHERE aspnet_Paths.ApplicationId = @ApplicationId " +
                    "AND aspnet_PersonalizationPerUser.PathId = aspnet_Paths.PathId " +
                    "AND (@Path IS NULL OR aspnet_Paths.LoweredPath LIKE @Path) " +
                    ") AS UserDataPerPath " +
                    "ON SharedDataPerPath.PathId = UserDataPerPath.PathId " +
                    ") " +
                    "WHERE aspnet_Paths.PathId = SharedDataPerPath.PathId OR aspnet_Paths.PathId = UserDataPerPath.PathId " +
                    "ORDER BY aspnet_Paths.Path ASC)";

                cmd = new MySqlCommand(sQuery, holder.Connection);

                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
                cmd.Parameters.Add(CreateInputParam("@Path", MySqlDbType.VarChar, Path.ToLower()));

                totalRecords = cmd.ExecuteNonQuery();

                sQuery = "SELECT aspnet_Paths.Path, " +
                    "SharedDataPerPath.LastUpdatedDate, " +
                    "SharedDataPerPath.SharedDataLength, " +
                    "UserDataPerPath.UserDataLength, " +
                    "UserDataPerPath.UserCount " +
                    "FROM aspnet_Paths, " +
                    "((SELECT PageIndex.ItemId AS PathId, " +
                    "aspnet_PersonalizationAllUsers.LastUpdatedDate AS LastUpdatedDate, " +
                    "LENGTH(aspnet_PersonalizationAllUsers.PageSettings) AS SharedDataLength " +
                    "FROM aspnet_PersonalizationAllUsers, PageIndex " +
                    "WHERE aspnet_PersonalizationAllUsers.PathId = PageIndex.ItemId " +
                    "AND PageIndex.IndexId >= @PageLowerBound AND PageIndex.IndexId <= @PageUpperBound " +
                    ") AS SharedDataPerPath " +
                    "FULL OUTER JOIN " +
                    "(SELECT PageIndex.ItemId AS PathId, " +
                    "SUM(LENGTH(aspnet_PersonalizationPerUser.PageSettings)) AS UserDataLength, " +
                    "COUNT(*) AS UserCount " +
                    "FROM aspnet_PersonalizationPerUser, PageIndex " +
                    "WHERE aspnet_PersonalizationPerUser.PathId = PageIndex.ItemId " +
                    "AND PageIndex.IndexId >= @PageLowerBound AND PageIndex.IndexId <= @PageUpperBound " +
                    "GROUP BY PageIndex.ItemId " +
                    ") AS UserDataPerPath " +
                    "ON SharedDataPerPath.PathId = UserDataPerPath.PathId " +
                    ") " +
                    "WHERE aspnet_Paths.PathId = SharedDataPerPath.PathId OR aspnet_Paths.PathId = UserDataPerPath.PathId " +
                    "ORDER BY aspnet_Paths.Path ASC ";

                outcmd = new MySqlCommand(sQuery, holder.Connection);

                outcmd.Parameters.Add(CreateInputParam("@PageLowerBound", MySqlDbType.Int32, PageLowerBound));
                outcmd.Parameters.Add(CreateInputParam("@PageUpperBound", MySqlDbType.Int32, PageUpperBound));

                return totalRecords;
            }
            else
            {
                sQuery = "INSERT INTO PageIndex (ItemId) (" +
                    "SELECT aspnet_PersonalizationPerUser.Id " +
                    "FROM aspnet_PersonalizationPerUser, aspnet_Users, aspnet_Paths " +
                    "WHERE aspnet_Paths.ApplicationId = @ApplicationId " +
                    "AND aspnet_PersonalizationPerUser.UserId = aspnet_Users.UserId " +
                    "AND aspnet_PersonalizationPerUser.PathId = aspnet_Paths.PathId " +
                    "AND (@Path IS NULL OR aspnet_Paths.LoweredPath LIKE @Path) " +
                    "AND (@UserName IS NULL OR aspnet_Users.LoweredUserName LIKE @UserName) " +
                    "AND (@InactiveSinceDate IS NULL OR aspnet_Users.LastActivityDate <= @InactiveSinceDate) " +
                    "ORDER BY aspnet_Paths.Path ASC, aspnet_Users.UserName ASC )";

                cmd = new MySqlCommand(sQuery, holder.Connection);

                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
                cmd.Parameters.Add(CreateInputParam("@Path", MySqlDbType.VarChar, Path.ToLower()));
                cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
                cmd.Parameters.Add(CreateInputParam("@InactiveSinceDate", MySqlDbType.DateTime, InactiveSinceDate));

                totalRecords = cmd.ExecuteNonQuery();

                sQuery = "SELECT aspnet_Paths.Path, aspnet_PersonalizationPerUser.LastUpdatedDate, LENGTH(aspnet_PersonalizationPerUser.PageSettings), aspnet_Users.UserName, aspnet_Users.LastActivityDate " +
                    "FROM aspnet_PersonalizationPerUser, aspnet_Users, aspnet_Paths, #PageIndex PageIndex " +
                    "WHERE aspnet_PersonalizationPerUser.Id = PageIndex.ItemId " +
                    "AND aspnet_PersonalizationPerUser.UserId = aspnet_Users.UserId " +
                    "AND aspnet_PersonalizationPerUser.PathId = aspnet_Paths.PathId " +
                    "AND PageIndex.IndexId >= @PageLowerBound AND PageIndex.IndexId <= @PageUpperBound " +
                    "ORDER BY aspnet_Paths.Path ASC, aspnet_Users.UserName ASC ";

                outcmd = new MySqlCommand(sQuery, holder.Connection);

                outcmd.Parameters.Add(CreateInputParam("@PageLowerBound", MySqlDbType.Int32, PageLowerBound));
                outcmd.Parameters.Add(CreateInputParam("@PageUpperBound", MySqlDbType.Int32, PageUpperBound));

                return totalRecords;
            }
        }

        internal static int aspnet_PersonalizationAdministration_GetCountOfState(Boolean AllUsersScope,
                String ApplicationName, String Path, String UserName, DateTime InactiveSinceDate,
                SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;
            int totalRecords = 0;

            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 0;
            }

            if (AllUsersScope)
            {
                sQuery = "SELECT COUNT(*) FROM aspnet_PersonalizationAllUsers, aspnet_Paths " +
                    "WHERE aspnet_Paths.ApplicationId = @ApplicationId " +
                    "AND aspnet_PersonalizationAllUsers.PathId = aspnet_Paths.PathId " +
                    "AND (@Path IS NULL OR aspnet_Paths.LoweredPath LIKE @Path) ";

                cmd = new MySqlCommand(sQuery, holder.Connection);

                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
                cmd.Parameters.Add(CreateInputParam("@Path", MySqlDbType.VarChar, Path.ToLower()));

                reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

                try
                {
                    if (reader.Read())
                    {
                        totalRecords = reader.GetInt32(0);
                    }
                }
                catch
                {

                }
                finally
                {
                    if (reader != null)
                    {
                        reader.Close();
                        reader = null;
                    }
                }
            }
            else
            {
                sQuery = "SELECT COUNT(*) " +
                    "FROM aspnet_PersonalizationPerUser, aspnet_Users, aspnet_Paths " +
                    "WHERE aspnet_Paths.ApplicationId = @ApplicationId " +
                    "AND aspnet_PersonalizationPerUser.UserId = aspnet_Users.UserId " +
                    "AND aspnet_PersonalizationPerUser.PathId = aspnet_Paths.PathId " +
                    "AND (@Path IS NULL OR aspnet_Paths.LoweredPath LIKE @Path) " +
                    "AND (@UserName IS NULL OR aspnet_Users.LoweredUserName LIKE @UserName) " +
                    "AND (@InactiveSinceDate IS NULL OR aspnet_Users.LastActivityDate <= @InactiveSinceDate) ";

                cmd = new MySqlCommand(sQuery, holder.Connection);

                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
                cmd.Parameters.Add(CreateInputParam("@Path", MySqlDbType.VarChar, Path.ToLower()));
                cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
                cmd.Parameters.Add(CreateInputParam("@InactiveSinceDate", MySqlDbType.DateTime, InactiveSinceDate));
                
                reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

                try
                {
                    if (reader.Read())
                    {
                        totalRecords = reader.GetInt32(0);
                    }
                }
                catch
                {

                }
                finally
                {
                    if (reader != null)
                    {
                        reader.Close();
                        reader = null;
                    }
                }
            }
            return totalRecords;
        }

        internal static int aspnet_PersonalizationPerUser_GetPageSettings(String ApplicationName,
                String UserName, String Path, DateTime CurrentTimeUtc, SqlConnectionHolder holder,
                ref MySqlCommand outcmd)
        {
            String sQuery = "SELECT PathId FROM aspnet_PersonalizationPerUser WHERE 1=0";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);


            Guid ApplicationId = Guid.Empty;
            Guid PathId = Guid.Empty;
            Guid UserId = Guid.Empty;

            sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 0;
            }


            sQuery = "SELECT UserId FROM aspnet_Users WHERE LoweredUserName = @UserName AND ApplicationId = @ApplicationId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            if (UserId.Equals(Guid.Empty))
            {
                return 0;
            }

            sQuery = "SELECT aspnet_Paths.PathId FROM aspnet_Paths WHERE aspnet_Paths.ApplicationId = @ApplicationId AND aspnet_Paths.LoweredPath = @Path";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@Path", MySqlDbType.VarChar, Path.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    PathId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            if (PathId.Equals(Guid.Empty))
            {
                return 0;
            }

            sQuery = "UPDATE aspnet_Users SET LastActivityDate = @CurrentTimeUtc WHERE UserId = @UserId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", MySqlDbType.DateTime, CurrentTimeUtc));
            cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

            cmd.ExecuteNonQuery();

            sQuery = "SELECT aspnet_PersonalizationPerUser.PageSettings FROM aspnet_PersonalizationPerUser " +
                "WHERE aspnet_PersonalizationPerUser.PathId = @PathId AND " +
                "aspnet_PersonalizationPerUser.UserId = @UserId";

            outcmd = new MySqlCommand(sQuery, holder.Connection);

            outcmd.Parameters.Add(CreateInputParam("@PathId", MySqlDbType.Binary, PathId));
            outcmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

            return 0;
        }

        internal static int aspnet_PersonalizationAllUsers_GetPageSettings(String ApplicationName,
                String Path, SqlConnectionHolder holder, ref MySqlCommand outcmd)
        {
            String sQuery = "SELECT PathId FROM aspnet_PersonalizationPerUser WHERE 1=0";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            Guid ApplicationId = Guid.Empty;
            Guid PathId = Guid.Empty;

            sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 0;
            }

            sQuery = "SELECT aspnet_Paths.PathId FROM aspnet_Paths WHERE aspnet_Paths.ApplicationId = @ApplicationId AND aspnet_Paths.LoweredPath = @Path";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@Path", MySqlDbType.VarChar, Path.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    PathId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            if (PathId.Equals(Guid.Empty))
            {
                return 0;
            }

            sQuery = "SELECT aspnet_PersonalizationAllUsers.PageSettings FROM aspnet_PersonalizationAllUsers " +
                    "WHERE aspnet_PersonalizationAllUsers.PathId = @PathId";
            outcmd = new MySqlCommand(sQuery, holder.Connection);

            outcmd.Parameters.Add(CreateInputParam("@PathId", MySqlDbType.Binary, PathId));

            return 0;
        }

        internal static int aspnet_PersonalizationPerUser_ResetPageSettings(String ApplicationName,
                String UserName, String Path, DateTime CurrentTimeUtc, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;
            Guid PathId = Guid.Empty;
            Guid UserId = Guid.Empty;

            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 1;
            }


            sQuery = "SELECT UserId FROM aspnet_Users WHERE LoweredUserName = @UserName AND ApplicationId = @ApplicationId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            if (UserId.Equals(Guid.Empty))
            {
                return 1;
            }

            sQuery = "SELECT aspnet_Paths.PathId FROM aspnet_Paths WHERE aspnet_Paths.ApplicationId = @ApplicationId AND aspnet_Paths.LoweredPath = @Path";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@Path", MySqlDbType.VarChar, Path.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    PathId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            if (PathId.Equals(Guid.Empty))
            {
                return 1;
            }

            sQuery = "UPDATE aspnet_Users SET LastActivityDate = @CurrentTimeUtc WHERE UserId = @UserId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", MySqlDbType.DateTime, CurrentTimeUtc));
            cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

            cmd.ExecuteNonQuery();

            sQuery = "DELETE FROM aspnet_PersonalizationPerUser WHERE PathId = @PathId AND UserId = @UserId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@PathId", MySqlDbType.Binary, PathId));
            cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

            cmd.ExecuteNonQuery();

            return 0;
        }

        internal static int aspnet_PersonalizationAllUsers_ResetPageSettings(String ApplicationName,
                String Path, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;
            Guid PathId = Guid.Empty;

            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 1;
            }

            sQuery = "SELECT aspnet_Paths.PathId FROM aspnet_Paths WHERE aspnet_Paths.ApplicationId = @ApplicationId AND aspnet_Paths.LoweredPath = @Path";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@Path", MySqlDbType.VarChar, Path.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    PathId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            if (PathId.Equals(Guid.Empty))
            {
                return 1;
            }

            sQuery = "DELETE FROM aspnet_PersonalizationAllUsers WHERE PathId = @PathId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@PathId", MySqlDbType.Binary, PathId));

            cmd.ExecuteNonQuery();

            return 0;
        }

        internal static int aspnet_PersonalizationAdministration_DeleteAllState(Boolean AllUsersScope,
                String ApplicationName, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;
            int totalRecords = 0;

            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 0;
            }

            if (AllUsersScope)
            {
                sQuery = "DELETE FROM aspnet_PersonalizationAllUsers WHERE PathId IN " +
                    "(SELECT aspnet_Paths.PathId " +
                    "FROM aspnet_Paths " +
                    "WHERE aspnet_Paths.ApplicationId = @ApplicationId) ";

                cmd = new MySqlCommand(sQuery, holder.Connection);

                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

                totalRecords = cmd.ExecuteNonQuery();
            }
            else
            {
                sQuery = "DELETE FROM aspnet_PersonalizationPerUser WHERE PathId IN " +
                    "(SELECT aspnet_Paths.PathId " +
                    "FROM aspnet_Paths " +
                    "WHERE aspnet_Paths.ApplicationId = @ApplicationId) ";

                cmd = new MySqlCommand(sQuery, holder.Connection);

                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

                totalRecords = cmd.ExecuteNonQuery();
            }
            return totalRecords;
        }

        internal static int aspnet_PersonalizationAdministration_ResetSharedState(String ApplicationName,
                String[] Paths, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;
            int totalRecords = 0;

            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 0;
            }

            foreach (String Path in Paths)
            {
                sQuery = "DELETE FROM aspnet_PersonalizationAllUsers WHERE PathId IN " +
                "(SELECT aspnet_PersonalizationAllUsers.PathId " +
                "FROM aspnet_PersonalizationAllUsers, aspnet_Paths " +
                "WHERE aspnet_Paths.ApplicationId = @ApplicationId " +
                "AND aspnet_PersonalizationAllUsers.PathId = aspnet_Paths.PathId " +
                "AND aspnet_Paths.LoweredPath = @Path) ";

                cmd = new MySqlCommand(sQuery, holder.Connection);

                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
                cmd.Parameters.Add(CreateInputParam("@Path", MySqlDbType.VarChar, Path.ToLower()));

                totalRecords += cmd.ExecuteNonQuery();
            }


            

            return totalRecords;
        }

        internal static int aspnet_PersonalizationAdministration_ResetUserState(String ApplicationName,
                DateTime InactiveSinceDate, String[] Users, String[] Paths, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;
            int totalRecords = 0;

            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return 0;
            }

            foreach (String UserName in Users)
            {
                foreach (String Path in Paths)
                {
                    sQuery = "DELETE FROM aspnet_PersonalizationPerUser " +
                        "WHERE Id IN (SELECT aspnet_PersonalizationPerUser.Id " +
                        "FROM aspnet_PersonalizationPerUser, aspnet_Users, aspnet_Paths " +
                        "WHERE Paths.ApplicationId = @ApplicationId " +
                        "AND aspnet_PersonalizationPerUser.UserId = aspnet_Users.UserId " +
                        "AND aspnet_PersonalizationPerUser.PathId = aspnet_Paths.PathId " +
                        "AND (@InactiveSinceDate IS NULL OR Users.LastActivityDate <= @InactiveSinceDate) " +
                        "AND (@UserName IS NULL OR aspnet_Users.LoweredUserName = @UserName) " +
                        "AND (@Path IS NULL OR aspnet_Paths.LoweredPath = @Path)) ";

                    cmd = new MySqlCommand(sQuery, holder.Connection);

                    cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
                    cmd.Parameters.Add(CreateInputParam("@InactiveSinceDate", MySqlDbType.DateTime, InactiveSinceDate));
                    cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
                    cmd.Parameters.Add(CreateInputParam("@Path", MySqlDbType.VarChar, Path.ToLower()));

                    totalRecords += cmd.ExecuteNonQuery();
                }
            }
            return totalRecords; 
        }

        internal static Guid aspnet_Paths_CreatePath(Guid ApplicationId, String Path, SqlConnectionHolder holder)
        {
            Guid PathId = Guid.Empty;

            String sQuery = "SELECT aspnet_Paths.PathId FROM aspnet_Paths WHERE aspnet_Paths.ApplicationId = @ApplicationId AND aspnet_Paths.LoweredPath = @Path";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@Path", MySqlDbType.VarChar, Path.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            MySqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    PathId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            if (PathId.Equals(Guid.Empty))
            {
                PathId = Guid.NewGuid();

                sQuery = "INSERT aspnet_Paths (PathId, ApplicationId, Path, LoweredPath) VALUES  " +
                       "(@PathId, @ApplicationId, @Path, @LoweredPath)";

                cmd = new MySqlCommand(sQuery, holder.Connection);

                cmd.Parameters.Add(CreateInputParam("@PathId", MySqlDbType.Binary, PathId));
                cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));
                cmd.Parameters.Add(CreateInputParam("@Path", MySqlDbType.VarChar, Path));
                cmd.Parameters.Add(CreateInputParam("@LoweredPath", MySqlDbType.VarChar, Path.ToLower()));

                cmd.ExecuteNonQuery();
            }

            return PathId;
        }


        internal static int aspnet_PersonalizationPerUser_SetPageSettings(String ApplicationName,
                String UserName, String Path, Byte[] PageSettings, DateTime CurrentTimeUtc,
                SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;
            Guid PathId = Guid.Empty;
            Guid UserId = Guid.Empty;

            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                ApplicationId = aspnet_Applications_CreateApplication(holder, ApplicationName);
            }


            sQuery = "SELECT UserId FROM aspnet_Users WHERE LoweredUserName = @UserName AND ApplicationId = @ApplicationId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            if (UserId.Equals(Guid.Empty))
            {
                UserId = aspnet_Users_CreateUser(ApplicationId, UserName, false, CurrentTimeUtc, holder);
            }

            sQuery = "SELECT aspnet_Paths.PathId FROM aspnet_Paths WHERE aspnet_Paths.ApplicationId = @ApplicationId AND aspnet_Paths.LoweredPath = @Path";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@Path", MySqlDbType.VarChar, Path.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    PathId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            if (PathId.Equals(Guid.Empty))
            {
                PathId = aspnet_Paths_CreatePath(ApplicationId, Path, holder);
            }

            sQuery = "UPDATE aspnet_Users SET LastActivityDate = @CurrentTimeUtc WHERE UserId = @UserId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", MySqlDbType.DateTime, CurrentTimeUtc));
            cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

            cmd.ExecuteNonQuery();

            sQuery = "SELECT COUNT(PathId) FROM aspnet_PersonalizationPerUser WHERE UserId = @UserId AND PathId = @PathId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
            cmd.Parameters.Add(CreateInputParam("@PathId", MySqlDbType.Binary, PathId));

            if ((long)cmd.ExecuteScalar() > 0) 
            {
                sQuery = "UPDATE aspnet_PersonalizationPerUser SET PageSettings = @PageSettings, " +
                        "LastUpdatedDate = @CurrentTimeUtc WHERE UserId = @UserId AND PathId = @PathId";
            }
            else
            {
                sQuery = "INSERT INTO aspnet_PersonalizationPerUser(UserId, PathId, PageSettings, " +
                        "LastUpdatedDate) VALUES (@UserId, @PathId, @PageSettings, @CurrentTimeUtc)";
            }

            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@PageSettings", MySqlDbType.Blob, PageSettings));
            cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", MySqlDbType.DateTime, CurrentTimeUtc));
            cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));
            cmd.Parameters.Add(CreateInputParam("@PathId", MySqlDbType.Binary, PathId));

            cmd.ExecuteNonQuery();

            return 0;
        }

        internal static int aspnet_PersonalizationAllUsers_SetPageSettings(String ApplicationName,
                String Path, Byte[] PageSettings, DateTime CurrentTimeUtc, SqlConnectionHolder holder)
        {
            Guid ApplicationId = Guid.Empty;
            Guid PathId = Guid.Empty;
            
            String sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                ApplicationId = aspnet_Applications_CreateApplication(holder, ApplicationName);
            }

            sQuery = "SELECT aspnet_Paths.PathId FROM aspnet_Paths WHERE aspnet_Paths.ApplicationId = @ApplicationId AND aspnet_Paths.LoweredPath = @Path";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@Path", MySqlDbType.VarChar, Path.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    PathId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            if (PathId.Equals(Guid.Empty))
            {
                PathId = aspnet_Paths_CreatePath(ApplicationId, Path, holder);
            }

            sQuery = "SELECT COUNT(PathId) FROM aspnet_PersonalizationAllUsers WHERE PathId = @PathId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@PathId", MySqlDbType.Binary, PathId));

            if ((long)cmd.ExecuteScalar() > 0) 
            {
                sQuery = "UPDATE aspnet_PersonalizationAllUsers SET PageSettings = @PageSettings, " +
                        "LastUpdatedDate = @CurrentTimeUtc WHERE PathId = @PathId";
            }
            else
            {
                sQuery = "INSERT INTO aspnet_PersonalizationAllUsers(PathId, PageSettings, " +
                    "LastUpdatedDate) VALUES (@PathId, @PageSettings, @CurrentTimeUtc)";
            }

            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@PathId", MySqlDbType.Binary, PathId));
            cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", MySqlDbType.DateTime, CurrentTimeUtc));
            cmd.Parameters.Add(CreateInputParam("@PageSettings", MySqlDbType.Blob, PageSettings));

            return 0;
        }

        internal static int aspnet_Membership_GetUserByUserId(Guid UserId, DateTime CurrentTimeUtc,
                Boolean UpdateLastActivity, SqlConnectionHolder holder, ref MySqlCommand outcmd)
        {
            String sQuery = "SELECT UserId FROM aspnet_Membership WHERE 1=0";
            outcmd = new MySqlCommand(sQuery, holder.Connection);
            
            MySqlCommand cmd;

            if (UpdateLastActivity)
            {
                sQuery = "UPDATE   aspnet_Users SET LastActivityDate = @CurrentTimeUtc " +
                    "FROM     aspnet_Users  WHERE    @UserId = UserId";
                cmd = new MySqlCommand(sQuery, holder.Connection);

                cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", MySqlDbType.DateTime, CurrentTimeUtc));
                cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                if ((long)cmd.ExecuteNonQuery() == 0)
                {
                    return -1;
                }
            }

            sQuery = "SELECT  aspnet_Membership.Email, aspnet_Membership.PasswordQuestion, aspnet_Membership.Comment, " +
                "aspnet_Membership.IsApproved, aspnet_Membership.CreateDate, aspnet_Membership.LastLoginDate, " +
                "aspnet_Users.LastActivityDate, aspnet_Membership.LastPasswordChangedDate, aspnet_Users.UserName, " +
                "aspnet_Membership.IsLockedOut, aspnet_Membership.LastLockoutDate " +
                "FROM    aspnet_Users, aspnet_Membership " +
                "WHERE   @UserId = aspnet_Users.UserId AND aspnet_Users.UserId = aspnet_Membership.UserId ";

            outcmd = new MySqlCommand(sQuery, holder.Connection);

            outcmd.Parameters.Add(CreateInputParam("UserId", MySqlDbType.Binary, UserId));

            return 0;
        }

        internal static int aspnet_Membership_GetUserByName(String ApplicationName, String UserName, 
                DateTime CurrentTimeUtc, Boolean UpdateLastActivity, SqlConnectionHolder holder,
                ref MySqlCommand outcmd)
        {
            String sQuery = "SELECT UserId FROM aspnet_Membership WHERE 1=0";
            outcmd = new MySqlCommand(sQuery, holder.Connection);

            Guid UserId = Guid.Empty;
            Guid ApplicationId = Guid.Empty;

            sQuery = "SELECT  ApplicationId FROM aspnet_Applications WHERE @ApplicationName = LoweredApplicationName";
            MySqlCommand cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));

            MySqlDataReader reader = null;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    ApplicationId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }

            if (ApplicationId.Equals(Guid.Empty))
            {
                return -1;
            }
            
            sQuery = "SELECT UserId FROM aspnet_Users WHERE LoweredUserName = @UserName AND ApplicationId = @ApplicationId";
            cmd = new MySqlCommand(sQuery, holder.Connection);

            cmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));
            cmd.Parameters.Add(CreateInputParam("@ApplicationId", MySqlDbType.Binary, ApplicationId));

            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

            try
            {
                if (reader.Read())
                {
                    UserId = reader.GetGuid(0);
                }
            }
            catch
            {

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                    reader = null;
                }
            }
            if (UserId.Equals(Guid.Empty))
            {
                return -1;
            }

            if (UpdateLastActivity)
            {
                sQuery = "UPDATE   aspnet_Users SET LastActivityDate = @CurrentTimeUtc " +
                    "FROM     aspnet_Users  WHERE    @UserId = UserId";
                cmd = new MySqlCommand(sQuery, holder.Connection);

                cmd.Parameters.Add(CreateInputParam("@CurrentTimeUtc", MySqlDbType.DateTime, CurrentTimeUtc));
                cmd.Parameters.Add(CreateInputParam("@UserId", MySqlDbType.Binary, UserId));

                if ((long)cmd.ExecuteNonQuery() == 0)
                {
                    return -1;
                }
            }

            sQuery = "SELECT aspnet_Membership.Email, aspnet_Membership.PasswordQuestion, aspnet_Membership.Comment, " +
                "aspnet_Membership.IsApproved, aspnet_Membership.CreateDate, aspnet_Membership.LastLoginDate, " +
                "aspnet_Users.LastActivityDate, aspnet_Membership.LastPasswordChangedDate, " +
                "aspnet_Users.UserId, aspnet_Membership.IsLockedOut, aspnet_Membership.LastLockoutDate " +
                "FROM    aspnet_Applications, aspnet_Users, aspnet_Membership " +
                "WHERE    @ApplicationName = aspnet_Applications.LoweredApplicationName AND " +
                "aspnet_Users.ApplicationId = aspnet_Applications.ApplicationId    AND " +
                "@UserName = aspnet_Users.LoweredUserName AND aspnet_Users.UserId = aspnet_Membership.UserId LIMIT 0,1 ";

            outcmd = new MySqlCommand(sQuery, holder.Connection);

            outcmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));
            outcmd.Parameters.Add(CreateInputParam("@UserName", MySqlDbType.VarChar, UserName.ToLower()));

            return 0;
        }

        internal static int aspnet_Membership_GetUserByEmail(String ApplicationName, String Email,
                SqlConnectionHolder holder, ref MySqlCommand outcmd)
        {
            String sQuery = "SELECT UserId FROM aspnet_Membership WHERE 1=0";
            outcmd = new MySqlCommand(sQuery, holder.Connection);

            if (String.IsNullOrEmpty(Email))
            {
                sQuery = "SELECT  aspnet_Users.UserName " +
                    "FROM    aspnet_Applications, aspnet_Users, aspnet_Membership " +
                    "WHERE   @ApplicationName = aspnet_Applications.LoweredApplicationName AND " +
                    "aspnet_Users.ApplicationId = aspnet_Applications.ApplicationId    AND " +
                    "aspnet_Users.UserId = aspnet_Membership.UserId AND " +
                    "aspnet_Membership.LoweredEmail IS NULL";
            }
            else
            {
                sQuery = "SELECT  u.UserName " +
                    "FROM    aspnet_Applications, aspnet_Users, aspnet_Membership " +
                    "WHERE   @ApplicationName = aspnet_Applications.LoweredApplicationName AND " +
                    "aspnet_Users.ApplicationId = aspnet_Applications.ApplicationId    AND " +
                    "aspnet_Users.UserId = aspnet_Membership.UserId AND " +
                    "@Email = aspnet_Membership.LoweredEmail";
            }

            outcmd = new MySqlCommand(sQuery, holder.Connection);

            outcmd.Parameters.Add(CreateInputParam("@ApplicationName", MySqlDbType.VarChar, ApplicationName.ToLower()));
            outcmd.Parameters.Add(CreateInputParam("@Email", MySqlDbType.VarChar, Email.ToLower()));

            return 0;
        }


        internal static MySqlParameter CreateInputParam(string paramName,
                                               MySqlDbType dbType,
                                               object objValue)
        {

            MySqlParameter param = new MySqlParameter(paramName, dbType);

            if (objValue == null)
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
    }
}
