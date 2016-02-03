using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Configuration;
using MySql.Data.MySqlClient;
using FirebirdSql.Data.FirebirdClient;

namespace SQLHelperv2
{

   /// <summary>
   /// Enumerador que especifica os tipos de bancos de dados.
   /// </summary>
   public enum TipoBanco
   { /// <summary>
      /// SqlServer
      /// </summary>
      SqlServer,
      /// <summary>
      /// MySQL
      /// </summary>
      MySql5,
      /// <summary>
      /// Firebird
      /// </summary>
      Firebird
   };

   /// <summary>
   /// Classe genérica para acesso a dados.
   /// </summary>
   public class BD
   {
      #region atributos
      private TipoBanco tipobanco;
      private string host;
      private string database;
      private string user;
      private string pass;
      #endregion

      #region propriedades
      public TipoBanco Tipobanco
      {
         get { return tipobanco; }
         set { tipobanco = value; }
      }
      public string Host
      {
         get { return host; }
         set { host = value; }
      }
      public string Database
      {
         get { return database; }
         set { database = value; }
      }
      public string User
      {
         get { return user; }
         set { user = value; }
      }
      public string Pass
      {
         get { return pass; }
         set { pass = value; }
      }
      #endregion

      #region construtores
      public BD(string tipobanco, string host, string database, string user, string pass)
      {
         switch (tipobanco)
         {
            default: Tipobanco = TipoBanco.SqlServer; break;
            case "firebird": Tipobanco = TipoBanco.Firebird; break;
            case "mysql": Tipobanco = TipoBanco.MySql5; break;
         }

         Host = host;
         Database = database;
         User = user;
         Pass = pass;

         VerificaNulos();
      }

      public BD()
      {
         Tipobanco = TipobancoConfig;
         Host = HostConfig;
         Database = DatabaseConfig;
         User = UserConfig;
         Pass = PassConfig;

         VerificaNulos();
      }
      #endregion

      #region Obtem credenciais direto do WebConfig/App.config
      public TipoBanco TipobancoConfig
      {
         get
         {
            string tp = ConfigurationSettings.AppSettings["tipobanco"];
            switch (tp)
            {
               case "firebird": return TipoBanco.Firebird;
               case "mysql": return TipoBanco.MySql5;
               case "sql": return TipoBanco.SqlServer;
            }
            return TipoBanco.SqlServer;
         }
      }
      public string HostConfig
      {
         get { return ConfigurationSettings.AppSettings["host"]; }
      }
      public string DatabaseConfig
      {
         get { return ConfigurationSettings.AppSettings["database"]; }
      }
      public string UserConfig
      {
         get { return (ConfigurationSettings.AppSettings["user"]); }
      }
      public string PassConfig
      {
         get { return (ConfigurationSettings.AppSettings["pass"]); }
      }
      #endregion

      private void VerificaNulos()
      {
         if (Tipobanco == TipoBanco.Firebird)
         {
            if (string.IsNullOrEmpty(User))
               User = "SYSDBA";

            if (string.IsNullOrEmpty(Pass))
               Pass = "roubaram";
         }

         if (Tipobanco == TipoBanco.MySql5)
         {
            if (string.IsNullOrEmpty(User))
               User = "root";

            if (string.IsNullOrEmpty(Pass))
               Pass = "123456big";
         }

         if (Tipobanco == TipoBanco.SqlServer)
         {
             if (string.IsNullOrEmpty(User))
                 User = "sa";

             if (string.IsNullOrEmpty(Pass))
                 Pass = "1@allebagord";
         }
      }

      private DbConnection GetBDConection()
      {
         try
         {
            switch (this.Tipobanco)
            {
               case TipoBanco.SqlServer:
                  return new SqlConnection(GeraConexString());
               case TipoBanco.MySql5:
                  return new MySqlConnection(GeraConexString());
               case TipoBanco.Firebird:
                  return new FbConnection(GeraConexString());
            }
            throw new Exception("Banco de dados não implementado");
         }
         catch (Exception ex)
         {
            throw new Exception("Não foi possível criar o database. Detalhes: " + ex.Message);
         }
      }

      private DbDataAdapter GetDataAdapter()
      {
         try
         {
            switch (this.Tipobanco)
            {
               case TipoBanco.SqlServer:
                  return new SqlDataAdapter(new SqlCommand());
               case TipoBanco.MySql5:
                  return new MySqlDataAdapter(new MySqlCommand());
               case TipoBanco.Firebird:
                  return new FbDataAdapter(new FbCommand());
            }
            throw new Exception("Banco de dados não implementado");
         }
         catch (Exception ex)
         {
            throw new Exception("Não foi possível criar o database. Detalhes: " + ex.Message);
         }
      }

      /// <summary>
      /// Gera a String de conexão de acordo com o banco de dados.
      /// </summary>
      /// <remarks>Todos dados necessarios para montar a string vem dos atributos</remarks>
      /// <returns>Retorna a String de conexão completa</returns>
      private string GeraConexString()
      {
         switch (this.Tipobanco)
         {
            case TipoBanco.SqlServer:
               SqlConnectionStringBuilder str = new SqlConnectionStringBuilder();
               str.DataSource = "localhost";
               str.InitialCatalog = "BELLA_22_01";
               if (string.IsNullOrEmpty(User))
                  str.IntegratedSecurity = true;
               else
               {
                  str.IntegratedSecurity = false;
                  str.UserID = User;
                  str.Password = Pass;
               }
               return str.ToString();

            case TipoBanco.MySql5:
               string connection = string.Format("Server={0};Database={1};Uid={2};Pwd={3};", this.Host, this.Database, this.User, this.Pass);
               MySqlConnectionStringBuilder csMy = new MySqlConnectionStringBuilder(connection);
               csMy.UseCompression = true;
               csMy.Pooling = true;
               csMy.MinimumPoolSize = 5;
               csMy.MaximumPoolSize = 10;
               return csMy.ToString();

            case TipoBanco.Firebird:
               FbConnectionStringBuilder cs = new FbConnectionStringBuilder();
               cs.DataSource = this.Host;
               cs.Database = this.Database;
               cs.UserID = this.User;
               cs.Password = this.Pass;
               cs.Dialect = 1;
               cs.IsolationLevel = IsolationLevel.ReadCommitted;
               cs.Pooling = false;
               cs.ConnectionLifeTime = 20;
               cs.ConnectionTimeout = 15;
               return cs.ToString();

         }
         return "";
      }

      /// <summary>
      /// Método para retornar um DataTable a partir de uma intrução sql.
      /// </summary>
      /// <param name="pComando">Comando SQL a ser executado contra o banco de dados.</param>
      /// <param name="ParamsList">Lista de Parametros contidos no comando SQL.</param>
      /// <returns>Retorna o DataTable carregado</returns>
      public DataTable GetDataTable(String pComando, SqlParamsList ParamsList)
      {
         DataTable dt = new DataTable();
         using (DbConnection DB = GetBDConection())
         {
            DbDataAdapter ad = GetDataAdapter();
            ad.SelectCommand.CommandText = pComando;
            ad.SelectCommand.Connection = DB;
            SetParams(ad.SelectCommand, ParamsList);
            DB.Open();
            try
            {
               ad.Fill(dt);
            }
            finally
            {
               DB.Close();
            }
         }
         return dt;
      }

      /// <summary>
      /// Método para retornar uma DataTable a partir de uma instrução sql que pode conter vários comandos (usado para o gerador de relatórios)
      /// </summary>
      /// <param name="pComandos">Lista de comandos a serem executados - O último gera o DataTable</param>
      /// <param name="ParamsList">Lista de parametros contidos nos comandos</param>
      /// <returns>Retorna o DataTable carregado</returns>
      public DataTable GetDataTableCmd(string[] pComandos, SqlParamsList ParamsList)
      {
         DataTable table = new DataTable();

         using (DbConnection DB = GetBDConection())
         {
            DB.Open();            

            try
            {
               for (int i = 0; i < pComandos.Length; i++)
               {                  
                  if (i == pComandos.Length - 1)
                  {
                     DbDataAdapter ad = GetDataAdapter();
                     ad.SelectCommand.CommandText = pComandos[i];
                     ad.SelectCommand.Connection = DB;                     
                     SetParams(ad.SelectCommand, ParamsList);
                     ad.Fill(table);
                  }
                  else
                  {
                     DbCommand cmd = DB.CreateCommand();
                     cmd.CommandText = pComandos[i];
                     SetParams(cmd, ParamsList);
                     cmd.ExecuteNonQuery();
                  }
               }
            }
            finally
            {
               DB.Close();               
            }
         }

         return table;
      }

      /// <summary>
      /// Método para retornar um DataSet a partir de uma instrucao sql
      /// </summary>
      /// <param name="pComando">Comando SQL a ser executado contra o banco de dados.</param>
      /// <param name="ParamsList">Lista de Parametros contidos no comando SQL.</param>
      /// <returns>Retorna o DataSet carregado</returns>
      public DataSet GetDataSet(String pComando, SqlParamsList ParamsList)
      {
         DataSet ds = new DataSet();
         ds.Tables.Add(GetDataTable(pComando, ParamsList));
         return ds;
      }

      /// <summary>
      /// Executa algum comando no banco(Insert, Update, Delete) 
      /// </summary>
      /// <param name="pComando">Comando SQL a ser executado contra o banco de dados.</param>
      /// <param name="ParamsList">Lista de Parametros contidos no comando SQL.</param>
      /// <returns>Retorna "1" se obtiver êxito na execução do comando</returns>
      public int ExecuteNoQuery(String pComando, SqlParamsList ParamsList)
      {
         int ret;
         using (DbConnection Con = GetBDConection())
         {
            DbCommand Cmd = Con.CreateCommand();
            Cmd.CommandText = pComando;
            SetParams(Cmd, ParamsList);
            Con.Open();
            try
            {
               ret = Cmd.ExecuteNonQuery();
            }
            finally
            {
               Con.Close();
            }
         }
         return ret;
      }

      /// <summary>
      /// Executa insert no banco 
      /// </summary>
      /// <param name="pComando">Comando SQL a ser executado contra o banco de dados.</param>
      /// <param name="ParamsList">Lista de Parametros contidos no comando SQL.</param>
      /// <returns>Retorna id do registro inserido</returns>
      public long InsertMySql(String pComando, SqlParamsList ParamsList)
      {
         MySqlCommand comando = new MySqlCommand();
         MySqlConnection conexao = (MySqlConnection)GetBDConection();

         try
         {
            comando.CommandText = pComando;
            comando.CommandType = CommandType.Text;
            SetParams(comando, ParamsList);
            comando.Connection = conexao;

            comando.Connection.Open();

            comando.ExecuteNonQuery();
            return comando.LastInsertedId;
         }
         finally
         {
            if (conexao.State != ConnectionState.Closed)
               conexao.Close();
         }
      }

      /// <summary>
      /// Método genérico para recuperar somente um campo do banco de dados.
      /// </summary>
      /// <typeparam name="T">Especifica o tipo do retorno esperado.</typeparam>
      /// <param name="pDefault">Informa o valor default de retorno caso o banco de dados nao retorne nada.</param>
      /// <param name="pComando">Comando SQL a ser executado contra o banco de dados.</param>
      /// <param name="ParamsList">Lista de Parametros contidos no comando SQL.</param>
      /// <returns>Retorna o valor com seu tipo(int, string, bool...) correspondente</returns>
      public T ExecuteScalar<T>(T pDefault, String pComando, SqlParamsList ParamsList)
      {

         object r = ExecuteScalar(pComando, ParamsList);
         if (r == null || r == DBNull.Value)
            return pDefault;
         try
         {
            r = Convert.ChangeType(r, typeof(T));
            return (T)r;
         }
         catch (Exception ex)
         {
            throw new Exception("Erro de Conversao: " + ex.Message);
         }

      }

      /// <summary>
      /// Método para recuperar somente um campo do banco de dados.
      /// </summary>
      /// <param name="pComando">Comando SQL a ser executado contra o banco de dados.</param>
      /// <param name="ParamsList">Lista de Parametros contidos no comando SQL.</param>
      /// <returns>Retorna um dado do Tipo Object</returns>
      public object ExecuteScalar(String pComando, SqlParamsList ParamsList)
      {
         object r;
         using (DbConnection Con = GetBDConection())
         {
            DbCommand Cmd = Con.CreateCommand();
            Cmd.CommandText = pComando;
            SetParams(Cmd, ParamsList);
            Con.Open();
            try
            {
               r = Cmd.ExecuteScalar();
            }
            finally
            {
               Con.Close();
            }

         }
         return r;
      }

      /// <summary>
      /// Método que retorna somente a primeira linha da busca realizada
      /// </summary>
      /// <param name="pComando">Comando SQL a ser executado contra o banco de dados.</param>
      /// <param name="ParamsList">Lista de Parametros contidos no comando SQL.</param>
      /// <returns>Retorna um DataRow contendo os dados trazidos do banco de dados.</returns>
      public DataRow GetOneRow(String pComando, SqlParamsList ParamsList)
      {
         DataTable dt = GetDataTable(pComando, ParamsList);
         if (dt.Rows.Count > 0)
         {
            return dt.Rows[0];
         }
         return null;
      }

      /// <summary>
      /// Metodo que realiza uma consulta rapida de somente leitura no banco de dados.
      /// </summary>
      /// <param name="pComando">Comando SQL a ser executado contra o banco de dados.</param>
      /// <param name="ParamsList">Lista de Parametros contidos no comando SQL.</param>
      /// <returns>Retorna um objeto SafeDataReader contendo os dados trazidos do banco de dados</returns>
      public SafeDataReader GetDataReader(string pComando, SqlParamsList ParamsList)
      {
         DbConnection Con = GetBDConection();
         DbCommand Cmd = Con.CreateCommand();
         Cmd.CommandText = pComando;
         SetParams(Cmd, ParamsList);
         Con.Open();
         return new SafeDataReader(Cmd.ExecuteReader(CommandBehavior.CloseConnection));
      }

      /// <summary>
      /// Metodo que atribui paramentros no Cmd(parametro) passado, conforme o Banco(FireBird, SQLServer, MySQL).
      /// </summary>
      /// <param name="Cmd">DBCommand que irá receber os paramentros</param>
      /// <param name="ParamsList">Lista dos paramentros.</param>
      /// <returns>Retorna um int, indicando a quantidade de parametros adicionados ao Cmd</returns>
      private int SetParams(IDbCommand Cmd, SqlParamsList ParamsList)
      {
         int iReturn = 0;

         if (ParamsList == null)
            return iReturn;

         char cDef = '@';
         char cNew = ' ';

         switch (this.Tipobanco)
         {
            case TipoBanco.SqlServer:
            case TipoBanco.Firebird:
               cNew = '@';
               break;
            case TipoBanco.MySql5:
               cNew = '?';
               break;
         }

         Cmd.CommandText = Cmd.CommandText.Replace(cDef, cNew);
         Cmd.Parameters.Clear();

         foreach (Fields Param in ParamsList)
         {
            switch (this.Tipobanco)
            {
               case TipoBanco.SqlServer:
                  SqlParameter ParamSQL = new SqlParameter(Param.Nome, Param.Valor);
                  Cmd.Parameters.Add(ParamSQL);
                  break;
               case TipoBanco.MySql5:
                  MySqlParameter ParamMy = new MySqlParameter(cNew + Param.Nome, Param.Valor);
                  if (Param.Valor is char)
                     ParamMy.DbType = DbType.String;
                  Cmd.Parameters.Add(ParamMy);
                  break;
               case TipoBanco.Firebird:
                  if (Param.Nome.Substring(0, 1) != "@")
                     Param.Nome = "@" + Param.Nome;

                  FbParameter ParamFb = new FbParameter();
                  ParamFb.ParameterName = Param.Nome;
                  ParamFb.Value = Param.Valor;
                  Cmd.Parameters.Add(ParamFb);
                  break;
            }
            iReturn++;
         }
         return iReturn;
      }
   }

   /// <summary>
   /// Classe para tratamento de valores null dos Gets do IDataReader 
   /// </summary>
   public class SafeDataReader : IDisposable
   {
      private IDataReader dr;

      public IDataReader Dr
      {
         get { return dr; }
         set { dr = value; }
      }

      public SafeDataReader(IDataReader datareader)
      {
         dr = datareader;
      }
      public SafeDataReader() { }

      public bool Read()
      {
         return dr.Read();
      }

      public void Close()
      {
         if (!dr.IsClosed)
            dr.Close();
      }

      public int FieldCount
      {
         get { return dr.FieldCount; }
      }

      public object GetObject(int i)
      {
         return dr[i];
      }

      public object GetObject(string fieldname)
      {
         return dr[fieldname];
      }

      public string GetString(int i)
      {
         try
         {
            if (dr.IsDBNull(i))
               return ("");
            else
               return dr.GetString(i);
         }
         catch
         {
            return ("");
         }
      }

      public string GetString(string fieldname)
      {
         return GetString(dr.GetOrdinal(fieldname));
      }

      public char GetChar(int i)
      {
         try
         {
            if (dr.IsDBNull(i))
               return ('\0');
            else
               return dr.GetChar(i);
         }
         catch
         {
            return ('\0');
         }
      }

      public char GetChar(string fieldname)
      {
         return dr.GetChar(dr.GetOrdinal(fieldname));
      }

      public float GetFloat(int i)
      {
         try
         {
            if (dr.IsDBNull(i))
               return (0);
            else
               return dr.GetFloat(i);
         }
         catch
         {
            return 0;
         }
      }

      public float GetFloat(string fieldname)
      {
         return GetFloat(dr.GetOrdinal(fieldname));
      }

      public int GetInt32(int i)
      {
         try
         {
            if (dr.IsDBNull(i))
               return (0);
            else
               return dr.GetInt32(i);
         }
         catch
         {
            return (0);
         }
      }

      public int GetInt32(string fieldname)
      {
         try
         {
            return dr.GetInt32(dr.GetOrdinal(fieldname));
         }
         catch
         {
            return dr.GetInt32(0);
         }
      }

      public Int64 GetInt64(int i)
      {
         try
         {
            if (dr.IsDBNull(i))
               return (0);
            else
               return dr.GetInt64(i);
         }
         catch
         {
            return (0);
         }
      }

      public Int64 GetInt64(string fieldname)
      {
         return GetInt64(dr.GetOrdinal(fieldname));
      }

      public System.DateTime GetDateTime(int i)
      {
         try
         {
            if (dr.IsDBNull(i))
               return System.DateTime.MinValue;
            else
               return dr.GetDateTime(i);
         }
         catch
         {
            return System.DateTime.MinValue;
         }

      }

      public System.DateTime GetDateTime(string fieldname)
      {
         return GetDateTime(dr.GetOrdinal(fieldname));
      }

      public double GetDouble(int i)
      {
         try
         {
            if (dr.IsDBNull(i))
               return 0;
            else
               return dr.GetDouble(i);
         }
         catch
         {
            return 0;
         }
      }

      public double GetDouble(string fieldname)
      {
         try
         {
            return dr.GetDouble(dr.GetOrdinal(fieldname));
         }
         catch
         {
            return dr.GetDouble(0);
         }

      }

      public decimal GetDecimal(int i)
      {
         try
         {
            if (dr.IsDBNull(i))
               return 0;
            else
               return dr.GetDecimal(i);
         }
         catch
         {
            return 0;
         }
      }

      public decimal GetDecimal(string fieldname)
      {
         return dr.GetDecimal(dr.GetOrdinal(fieldname));
      }

      public byte[] GetBytes(int i)
      {
         byte[] buffer = new byte[1];
         if (dr.IsDBNull(i))
            return buffer;
         else
         {
            long size = dr.GetBytes(i, 0, null, 0, int.MaxValue);
            buffer = new byte[size];
            dr.GetBytes(i, 0, buffer, 0, Convert.ToInt32(size));
            return buffer;
         }
      }

      public bool HasRows()
      {
         return ((FirebirdSql.Data.FirebirdClient.FbDataReader)(dr)).HasRows;
      }

      public void Dispose()
      {
         dr.Dispose();
      }
   }
}