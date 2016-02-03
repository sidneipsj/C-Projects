using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using SQLHelperv2;
using Negocio;
using Misc;

namespace DAL
{
   public class DALProgConv
   {
      public static bool IncluiProgConv(ProgConv pc, string operador)
      {                           
         MontadorSql mont = new MontadorSql("prog_conv", MontadorType.Insert);
         mont.AddField("prog_id", pc.Programa.Prog_id);
         mont.AddField("conv_id", pc.Conveniado.Conv_id);

         BD BD = new BD();
         int incluiu = BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());

         if (incluiu == 1)
         {
            int logID = Log.GeraLogID();
            //Log.GravaLog(logID,"FCadConv", "Programa", "", pc.Programa.Prog_id.ToString(), operador, "Inclusão", "Cadastro de Conveniados", pc.Conveniado.Conv_id.ToString(), "Conv ID: " + pc.Conveniado.Conv_id.ToString(), "");
            return true;
         }

         return false;
      }
   }
}
