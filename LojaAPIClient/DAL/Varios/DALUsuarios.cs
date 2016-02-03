using System;
using System.Collections.Generic;
using System.Text;
using Negocio;
using System.Data;
using SQLHelperv2;
using Misc;
using netUtil;

namespace DAL
{
   public class DALUsuarios
   { 
      //Alterado para SqlServer
      public static int GeraUsuarioId()
      {
         BD BD = new BD();
         return Convert.ToInt32(BD.ExecuteScalar("SELECT NEXT VALUE FOR SWEB_USU_ID ", null).ToString());
      }

      //Alterado para SqlServer
      public static int IncluirUsuario(Usuarios usuario, int nProtocolo)
      {
         try
         {
            usuario.Usu_id = GeraUsuarioId();

            MontadorSql mont = new MontadorSql("usuarios_web", MontadorType.Insert);
            mont.AddField("usu_id", usuario.Usu_id);
            mont.AddField("usu_nome", Utils.TirarAcento(usuario.Usu_nome).ToUpper());
            mont.AddField("usu_email", usuario.Usu_email);
            mont.AddField("usu_liberado", "S");
            Funcoes.Crypt cr = new Funcoes.Crypt();
            mont.AddField("usu_senha", cr.Crypt("E", "1111", "BIGCOMPRAS"));
            mont.AddField("usu_apagado", "N");
            mont.AddField("usu_tipo", usuario.Usu_tipo);
            mont.AddField("emp_for_id", usuario.Emp_for_id);

            BD BD = new BD();
            int incluiu = BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
            if (incluiu == 1)
            {
               string operador = "EM ADMINISTRADOR";
               if (usuario.Usu_tipo.Equals("1"))
                  operador = "ES ADMINISTRADOR";

               int logID = Log.GeraLogID();
               Log.GravaLog(logID, "FCadUsu", "usu_id", "", usuario.Usu_id.ToString(), operador, "Inclusão", "Cadastro de Usuários", usuario.Usu_id.ToString(), "Usu ID: " + usuario.Usu_id, "", nProtocolo);
            }
            else usuario.Usu_id = 0;

            return usuario.Usu_id;
         }
         catch (Exception ex)
         {
            throw new Exception("Erro ao incluir usuário. Erro: " + ex.Message);
         }
      }

      public static void AlterarUsuario(Usuarios Alt, Usuarios Ori)
      {
         try
         {
            string operador = "EM ADMINISTRADOR";
            if (Ori.Usu_tipo.Equals("1"))
               operador = "ES ADMINISTRADOR";

            if (Utils.TirarAcento(Alt.Usu_nome).ToUpper() != Ori.Usu_nome)
               ConfirmAlteracao("usu_nome", Utils.TirarAcento(Alt.Usu_nome).ToUpper(), Ori.Usu_id, Ori.Usu_nome, operador);            
            if (Alt.Usu_cpf != Ori.Usu_cpf)
               ConfirmAlteracao("usu_cpf", Alt.Usu_cpf, Ori.Usu_id, Ori.Usu_cpf, operador);
            if (Alt.Usu_email != Ori.Usu_email)
               ConfirmAlteracao("usu_email", Alt.Usu_email, Ori.Usu_id, Ori.Usu_email, operador);
            if (Alt.Usu_liberado != Ori.Usu_liberado)
               ConfirmAlteracao("usu_liberado", Alt.Usu_liberado, Ori.Usu_id, Ori.Usu_liberado, operador);            
         }
         catch (Exception e)
         {
            throw new Exception("Erro ao atualizar usuário: " + e.Message);
         }
      }

      private static void ConfirmAlteracao(string campo, object newval, int usu_id, object oldval, string operador)
      {
         MontadorSql mont = new MontadorSql("usuarios_web", MontadorType.Update);
         mont.AddField(campo, newval);
         mont.AddField("usu_dtalteracao", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"));
         mont.AddField("usu_operador", operador);
         mont.SetWhere("WHERE usu_id = " + usu_id, null);

         BD BD = new BD();
         if (BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams()) == 1)
         {
            if (newval == null) newval = "";
            if (oldval == null) oldval = "";
            int logID = Log.GeraLogID();
            //Log.GravaLog(logID,"FCadUsu", campo.Substring(4), oldval.ToString(), newval.ToString(), operador, "Alteração", "Cadastro de Usuários", usu_id.ToString(), "Usu ID: " + usu_id, "");
         }
      }

      public static bool ExcluirUsuario(int usu_id, string tipo, int nProtocolo)
      {
         try
         {
            MontadorSql mont = new MontadorSql("usuarios_web", MontadorType.Update);
            mont.AddField("usu_apagado", "S");
            mont.AddField("usu_liberado", "N");
            mont.AddField("usu_dtapagado", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"));
            mont.SetWhere("WHERE usu_id = " + usu_id, null);

            BD BD = new BD();
            int excluiu = BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());

            if (excluiu == 1)
            {
               string operador = "EM ADMINISTRADOR";
               if (tipo.Equals("1"))
                  operador = "ES ADMINISTRADOR";

               int logID = Log.GeraLogID();
               Log.GravaLog(logID,"FCadUsu", "apagado", "N", "S", operador, "Exclusão", "Cadastro de Usuários", usu_id.ToString(), "Usu ID: " + usu_id, "", nProtocolo);
            }

            return (excluiu == 1);
         }
         catch(Exception ex)
         {
            throw new Exception("Erro ao excluir usuário: " + ex.Message);
         }
      }

      //Alterado para SqlServer
      public static Usuarios GetUsuario(int usu_id)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("id", usu_id));         

         string sql = "SELECT usu_nome, usu_email, usu_liberado, usu_tipo";
         sql += " FROM Usuarios_Web";
         sql += " WHERE usu_id = @id";

         BD BD = new BD();
         DataRow row = BD.GetOneRow(sql, ps);

         Usuarios usuario = new Usuarios();
         usuario.Usu_id = usu_id;
         usuario.Usu_nome = row["usu_nome"].ToString();
         usuario.Usu_email = row["usu_email"].ToString();
         usuario.Usu_liberado = row["usu_liberado"].ToString();
         usuario.Usu_tipo = row["usu_tipo"].ToString();

         return usuario;
      }

      public static List<Usuarios> GetUsuarios(string tipo, int emp_for_id)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("tipo", tipo));
         ps.Add(new Fields("id", emp_for_id)); 

         string sql = "SELECT usu_id, usu_nome, usu_email, usu_liberado";
         sql += " FROM Usuarios_Web";
         sql += " WHERE usu_tipo = @tipo";
         sql += " AND emp_for_id = @id";
         sql += " AND usu_apagado <> 'S'";

         BD BD = new BD();
         DataTable dt = BD.GetDataTable(sql, ps);

         List<Usuarios> lista = new List<Usuarios>();

         foreach (DataRow row in dt.Rows)
         {
            Usuarios usuario = new Usuarios();
            usuario.Usu_id = Convert.ToInt32(row["usu_id"]);
            usuario.Usu_nome = row["usu_nome"].ToString();            
            usuario.Usu_email = row["usu_email"].ToString();
            usuario.Usu_liberado = row["usu_liberado"].ToString();
            lista.Add(usuario);
         }

         return lista;
      }

      public static Usuarios GetDadosLogin(string usu_email, string tipo)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("email", usu_email));
         ps.Add(new Fields("tipo", tipo));

         string sql = "SELECT usu.usu_id, usu.usu_nome, usu.usu_senha, usu.emp_for_id, tip.nome";
         if (tipo.Equals("0"))
            sql += ",tip.prog_desc, tip.inc_cart_pbm";
         sql += " FROM Usuarios_Web usu";
         if(tipo.Equals("0"))
            sql += " JOIN Empresas tip ON tip.empres_id = usu.emp_for_id";
         else
            sql += " JOIN Credenciados tip ON tip.cred_id = usu.emp_for_id";
         sql += " WHERE usu.usu_email = @email";
         sql += " AND usu.usu_tipo = @tipo";
         sql += " AND usu.usu_liberado <> 'N'";
         sql += " AND usu.usu_apagado <> 'S'";
         sql += " AND tip.apagado <> 'S'";

         BD BD = new BD();
         DataRow row = BD.GetOneRow(sql, ps);

         Usuarios usuario = new Usuarios();

         if (row != null)
         {
            usuario.Usu_id = Convert.ToInt32(row["usu_id"]);
            usuario.Usu_nome = row["usu_nome"].ToString();
            usuario.Usu_senha = row["usu_senha"].ToString();            
            usuario.Emp_for_id = Convert.ToInt32(row["emp_for_id"]);
            usuario.Emp_for_nome = row["nome"].ToString();
            if (tipo.Equals("0"))
            {
               usuario.Emp_tipo = row["prog_desc"].ToString();
               usuario.Emp_inc_cart_pbm = row["inc_cart_pbm"].ToString();
            }
         }
         else usuario.Usu_id = 0;

         return usuario;
      }      
 
      public static bool AlterarSenha(Usuarios usuario)
      {
         MontadorSql mont = new MontadorSql("usuarios_web", MontadorType.Update);
         mont.AddField("usu_senha", usuario.Usu_senha);
         mont.SetWhere("WHERE usu_id = " + usuario.Usu_id, null);

         int alterou = 0;

         try
         {
            BD BD = new BD();
            alterou = BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
         }
         catch (Exception e)
         {
            throw new Exception("Erro ao alterar senha: " + e.Message);
         }

         return (alterou == 1);
      }
 
      //Alterado para SqlServer
      public static bool VerificaCPFExiste(string cpf)
      {
         BD BD = new BD();
         return (BD.ExecuteScalar("SELECT usu_cpf FROM Usuarios_Web WHERE usu_cpf = '" + cpf + "' AND usu_apagado <> 'S'", null) != null);         
      }

      //Alterado para SqlServer
      public static bool VerificaEmailExiste(string email)
      {
         BD BD = new BD();
         return (BD.ExecuteScalar("SELECT usu_email FROM Usuarios_Web WHERE usu_email = '" + email + "' AND usu_apagado <> 'S'", null) != null);
      }

      public static bool VerificaEmailECpfExistenteNaEmpresa(string cpf, string email, int empresa)
      {
        BD BD = new BD();
        return (BD.ExecuteScalar("SELECT usu_email FROM usuarios_Web WHERE usu_email = '" + email + "' and emp_for_id = " + empresa.ToString() + " and usu_apagado <> 'S''" + email + "' AND usu_apagado <> 'S'", null) != null);
      }
   }
}
