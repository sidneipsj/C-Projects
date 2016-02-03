using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using SQLHelperv2;
using Negocio;

namespace DAL
{
   public static class DALRedes
   {
      public static List<Redes> ObterRedes()
      {
         string sql = @"SELECT id_rede, nome, ultima_comunicacao FROM Redes";
         
         BD BD = new BD();
         DataTable table = BD.GetDataTable(sql, null);

         List<Redes> redes = new List<Redes>();
         foreach (DataRow row in table.Rows)
         {
            Redes rede = new Redes();
            rede.Id_rede = Convert.ToInt32(row["id_rede"]);
            rede.Nome = row["nome"].ToString();
            if(!string.IsNullOrEmpty(row["ultima_comunicacao"].ToString()))
               rede.Ultima_comunicacao = Convert.ToDateTime(row["ultima_comunicacao"]);
            redes.Add(rede);
         }

         return redes;
      }

      public static int Incluir(string nome)
      {
         MontadorSql mont = new MontadorSql("redes", MontadorType.Insert);         
         mont.AddField("nome", nome);         

         int id = 0;

         try
         {
            BD BD = new BD();
            id = Convert.ToInt32(BD.ExecuteScalar(mont.GetSqlString() + " SELECT SCOPE_IDENTITY();", mont.GetParams()));
         }
         catch (Exception e)
         {
            throw new Exception("Erro ao incluir rede: " + e.Message);
         }

         return id;
      }

      public static void Alterar(Redes rede)
      {
         MontadorSql mont = new MontadorSql("redes", MontadorType.Update);
         mont.AddField("ultima_comunicacao", rede.Ultima_comunicacao);
         mont.SetWhere("WHERE id_rede=" + rede.Id_rede, null);

         try
         {
            BD BD = new BD();
            BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
         }
         catch (Exception e)
         {
            throw new Exception("Erro ao alterar rede: " + e.Message);
         }
      }
   }
}
