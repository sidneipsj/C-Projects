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
   public class DALPermissoes
   { 
      public static void IncluirPermissoes(int usu_id, string tipo)
      {
         try
         {
            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("tipo", tipo));
            ps.Add(new Fields("usuid", usu_id));

            string sql = "SELECT mod.mod_id";
            sql += " FROM Modulos_Web mod";
            sql += " WHERE mod.mod_id NOT IN (";
            sql += " SELECT mod_id FROM Permissoes_Web perm WHERE perm.usu_id = @usuid)";
            sql += " AND mod.mod_tipo = @tipo";

            BD BD = new BD();
            DataTable dt = BD.GetDataTable(sql, ps);

            foreach (DataRow row in dt.Rows)
            {
               MontadorSql mont = new MontadorSql("permissoes_web", MontadorType.Insert);
               mont.AddField("usu_id", usu_id);
               mont.AddField("mod_id", Convert.ToInt32(row["mod_id"]));
               mont.AddField("perm_permite", "S");
               
               BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());               
            }
         }
         catch (Exception ex)
         {
            throw new Exception("Erro ao incluir permissões: " + ex.Message);
         }
      }

      public static bool AlterarPermissao(Permissoes permissao)
      {
         try
         {
            MontadorSql mont = new MontadorSql("permissoes_web", MontadorType.Update);
            mont.AddField("perm_permite", permissao.Perm_permite);
            mont.SetWhere("WHERE usu_id = " + permissao.Usuario.Usu_id + " AND mod_id = " + permissao.Modulo.Mod_id, null);

            BD BD = new BD();
            int alterou = BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());

            return (alterou == 1);
         }
         catch (Exception ex)
         {
            throw new Exception("Erro ao alterar permissão: " + ex.Message);
         }
      }      

      public static bool GetPermissao(int usu_id, string mod_nome)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("usuid", usu_id));
         ps.Add(new Fields("modnome", mod_nome));         

         string sql = "SELECT perm.perm_permite";
         sql += " FROM Permissoes_Web perm";
         sql += " JOIN Modulos_Web mod ON perm.mod_id = mod.mod_id";
         sql += " WHERE perm.usu_id = @usuid";
         sql += " AND mod.mod_nome = @modnome";

         BD BD = new BD();
         DataRow row = BD.GetOneRow(sql, ps);

         Permissoes permissao = new Permissoes();         
         permissao.Perm_permite = row["perm_permite"].ToString();         

         return permissao.Perm_permite.Equals("S");
      }
      
      //Alterado para SqlServer
      public static List<Permissoes> GetPermissoesUsuario(int usu_id, string tipo)
      {
         IncluirPermissoes(usu_id, tipo);

         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("usuid", usu_id));

         string sql = "SELECT mod.mod_id, mod.mod_nome, perm.perm_permite";
         sql += " FROM Permissoes_Web perm";
         sql += " JOIN Modulos_Web mod ON perm.mod_id = mod.mod_id";
         sql += " WHERE perm.usu_id = @usuid";

         BD BD = new BD();
         DataTable dt = BD.GetDataTable(sql, ps);

         List<Permissoes> lista = new List<Permissoes>();

         foreach (DataRow row in dt.Rows)
         {
            Permissoes permissao = new Permissoes();
            permissao.Modulo.Mod_id = Convert.ToInt32(row["mod_id"]);
            permissao.Modulo.Mod_nome = row["mod_nome"].ToString();
            permissao.Perm_permite = row["perm_permite"].ToString();

            lista.Add(permissao);
         }
         return lista;
      }
   }
}
