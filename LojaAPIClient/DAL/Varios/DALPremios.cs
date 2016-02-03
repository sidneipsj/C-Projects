using System;
using System.Collections.Generic;
using System.Text;
using SQLHelperv2;
using Negocio;
using System.Data;

namespace DAL
{
   public class DALPremios
   {               
      public static List<Fidel_Premios> GetPremios()
      {
         string sql = " SELECT premio_id, descricao, pontos";
         sql += " FROM Fidel_Premios";
         sql += " WHERE apagado <> 'S'";

         List<Fidel_Premios> lista = new List<Fidel_Premios>();

         BD BD = new BD();
         SafeDataReader dr = BD.GetDataReader(sql, null);
         try
         {
             while (dr.Read())
             {
                 Fidel_Premios premio = new Fidel_Premios();
                 premio.Premio_id = dr.GetInt32("premio_id");
                 premio.Descricao = dr.GetString("descricao");
                 premio.Pontos = dr.GetInt32("pontos");
                 lista.Add(premio);
             }
         }
         finally
         {
             dr.Close();
         }

         return lista;
      }

      public static byte[] GetImagemPremio(int premio_id)
      {
         string sql = " SELECT imagem";
         sql += " FROM Fidel_Premios";
         sql += " WHERE premio_id=" + premio_id;

         BD BD = new BD();
         SafeDataReader dr = BD.GetDataReader(sql, null);
         try
         {
             dr.Read();

             byte[] imagem = dr.GetBytes(0);
             return imagem;
         }
         finally
         {
             dr.Close();
         }
      }
   }
}
