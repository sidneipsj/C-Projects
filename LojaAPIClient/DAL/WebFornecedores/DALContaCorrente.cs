using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using Negocio;
using SQLHelperv2;

namespace DAL
{
  public class DALContaCorrente
  {    
    public static DataTable GetInfoAutors(string pAutors)
    {
      string sql = "SELECT autorizacao_id, digito, data, debito FROM Contacorrente WHERE autorizacao_id IN (" + pAutors + ")";
      BD BD = new BD();
      return BD.GetDataTable(sql, null);
    }

    public string InsertNF(Autorizacoes autor)
    {
      string sql = " UPDATE Contacorrente SET receita = '" + autor.Comrec + "'";
      if (!string.IsNullOrEmpty(autor.Nf))
        sql += ", nf = '" + autor.Nf + "'";
      sql += " WHERE autorizacao_id = " + autor.Autorizacao_id;
      BD BD = new BD();
      int c = (int)BD.ExecuteNoQuery(sql, null);
      if (c == 1)
        return "Dados gravados com sucesso";
      else
        return "Não foi possível realizar a operação";

    }

    public Autorizacoes GetCCbyId(string autor_id)
    {
      try
      {
        string sql = "SELECT cred_id, credito, data, debito FROM Contacorrente WHERE autorizacao_id = " + autor_id;
        BD BD = new BD();
        SafeDataReader dr = BD.GetDataReader(sql, null);
        Autorizacoes autor = new Autorizacoes();
        try
        {
            if (dr.Read())
            {
                autor.Credenciado.Cred_id = dr.GetInt32("CRED_ID");
                autor.Credito = dr.GetFloat("CREDITO");
                autor.Data = dr.GetDateTime("DATA");
                autor.Debito = dr.GetFloat("DEBITO");
            }
        }
        finally
        {
            dr.Close();
        }
        return autor;
      }
      catch
      {
        throw new Exception("Não foi possível recuperar dados da ContaCorrente");
      }
    }


    public static string NumAutorizacao(string transID)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@transID", transID));
        BD BD = new BD();
        return BD.ExecuteScalar("select top 1 autorizacao_id from contacorrente where trans_id = @transID", ps).ToString();

    }

    public static string NumDigito(string transID)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@transID", transID));
        BD BD = new BD();
        return BD.ExecuteScalar("select top 1 digito from contacorrente where trans_id = @transID", ps).ToString();

    }


    public static string SNumAutorizacao(string transID)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@transID", transID));
        BD BD = new BD();
        return BD.ExecuteScalar("select top 1 autor_id from autor_transacoes where trans_id = @transID", ps).ToString();

    }

    public static string SNumDigito(string transID)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@transID", transID));
        BD BD = new BD();
        return BD.ExecuteScalar("select top 1 digito from autor_transacoes where trans_id = @transID", ps).ToString();

    }
  }
}
