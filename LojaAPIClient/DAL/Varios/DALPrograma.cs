using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Negocio;
using SQLHelperv2;

namespace DAL
{
   public class DALPrograma
   {
      //Alterado para SqlServer
      public static List<Programas> GetProgramas()
      {
         string sql = " SELECT prog.prog_id, prog.nome";
         sql += " FROM Programas prog";
         sql += " WHERE prog.apagado <> 'S'";
         sql += " AND prog.dt_inicio <= '" + DateTime.Now.ToString("dd/MM/yyyy") + "'";
         sql += " AND prog.dt_fim >= '" + DateTime.Now.ToString("dd/MM/yyyy") + "'";

         BD BD = new BD();
         DataTable table = BD.GetDataTable(sql, null);

         List<Programas> lista = new List<Programas>();

         foreach (DataRow row in table.Rows)
         {
            Programas prog = new Programas();
            prog.Prog_id = Convert.ToInt32(row["prog_id"]);
            prog.Nome = row["nome"].ToString();
            lista.Add(prog);
         }

         return lista;
      }

      //Alterado para SqlServer
      public static List<Programas> GetProgConvOuEmpr(int conv_id, int empres_id)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("convid", conv_id));
         ps.Add(new Fields("empresid", empres_id));

         string sql = " SELECT prog.prog_id, prog.nome";
         sql += " FROM Programas prog";
         sql += " WHERE prog.apagado <> 'S'";
         sql += " AND prog.dt_inicio <= '" + DateTime.Now.ToString("dd/MM/yyyy") + "'";
         sql += " AND prog.dt_fim >= '" + DateTime.Now.ToString("dd/MM/yyyy") + "'";         
         sql += " AND (prog.prog_id IN (";
         sql += "    SELECT prog_id FROM Prog_Empr WHERE empres_id = @empresid)";
         sql += " OR prog.prog_id IN (";
         sql += "    SELECT prog_id FROM Prog_Conv WHERE conv_id = @convid))";

         BD BD = new BD();
         DataTable table = BD.GetDataTable(sql, ps);

         List<Programas> lista = new List<Programas>();

         foreach (DataRow row in table.Rows)
         {
            Programas prog = new Programas();
            prog.Prog_id = Convert.ToInt32(row["prog_id"]);
            prog.Nome = row["nome"].ToString();
            lista.Add(prog);
         }

         return lista;
      }
   }
}
