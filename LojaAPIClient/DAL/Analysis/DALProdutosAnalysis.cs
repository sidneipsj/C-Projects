using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Data.OleDb;
using System.Data.SqlClient;
using Negocio;

namespace DAL
{
   public static class DALProdutosAnalysis
   {
      public static List<ProdutosAnalysis> ObterProdutosGuiaFarmacia(string arquivoMdb, string senha)
      {
         string connString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + arquivoMdb + ";";
         if (!string.IsNullOrEmpty(senha))
            connString += "Jet OLEDB:Database Password=" + senha + ";";
         OleDbConnection conn = new OleDbConnection(connString);
         conn.Open();

         try
         {
            DataTable table = new DataTable();

            string cmd = @"
            SELECT bar.CodBarras,
            prod.CodProd, prod.NomeProd, prod.APresenta, prod.Generico, prod.TipoLista,
            lab.Nomefant,
            clas.Classe,
            sclas.SubClasse,
            fam.Familia,
            pat.PATivo
            FROM ProdutosCB bar,
            Produtos prod,
            Laboratorios lab,
            Classe clas,
            SubClasse sclas,
            Familias fam,
            PAtivo pat
            WHERE prod.CodProd = bar.CodProd
            AND lab.Codlab = prod.Codlab
            AND clas.CodClas = prod.CodClas
            AND sclas.CodSClas = prod.CodSClas
            AND fam.CodFam = prod.CodFam
            AND pat.CodPAT = prod.CodPAT";
            
            OleDbDataAdapter adapter = new OleDbDataAdapter(cmd, conn);
            adapter.Fill(table);

            List<ProdutosAnalysis> produtos = new List<ProdutosAnalysis>();
            foreach (DataRow row in table.Rows)
            {
               ProdutosAnalysis produto = new ProdutosAnalysis();
               produto.Barras = row["CodBarras"].ToString();
               produto.Nome = row["NomeProd"].ToString();
               produto.Apresentacao = row["APresenta"].ToString();
               produto.Laboratorio = row["Nomefant"].ToString();
               produto.Classe = row["Classe"].ToString();
               produto.Subclasse = row["SubClasse"].ToString();
               produto.Generico = row["Generico"].ToString();
               produto.Lista = row["TipoLista"].ToString();
               produto.Familia = row["Familia"].ToString();
               produto.Principio_ativo = row["PATivo"].ToString();
               produto.Codigo_guia = Convert.ToInt32(row["CodProd"]);
               produtos.Add(produto);
            }
                          
            return produtos;
         }
         catch (Exception ex)
         {
            throw new Exception("Erro ao obter produtos da fonte: " + ex.Message);
         }
         finally
         {
            conn.Close();
         }
      }

      public static void InserirProdutosGuiaFarmacia(List<ProdutosAnalysis> produtos, string servidor, string database, string usuario, string senha)
      {
         SqlConnectionStringBuilder str = new SqlConnectionStringBuilder();
         str.DataSource = servidor;
         str.InitialCatalog = database;
         if (string.IsNullOrEmpty(usuario))
            str.IntegratedSecurity = true;
         else
         {
            str.IntegratedSecurity = false;
            str.UserID = usuario;
            str.Password = senha;
         }
         
         SqlConnection conn = new SqlConnection(str.ToString());
         conn.Open();
         SqlTransaction trans = conn.BeginTransaction();
         SqlCommand cmd = new SqlCommand();
         cmd.Transaction = trans;
         cmd.Connection = conn;

         try
         {
            foreach (ProdutosAnalysis produto in produtos)
            {
               cmd.Parameters.AddWithValue("@barras", produto.Barras);
               cmd.Parameters.AddWithValue("@nome", produto.Nome);
               cmd.Parameters.AddWithValue("@apresentacao", produto.Apresentacao);
               cmd.Parameters.AddWithValue("@laboratorio", produto.Laboratorio);
               cmd.Parameters.AddWithValue("@classe", produto.Classe);
               cmd.Parameters.AddWithValue("@subclasse", produto.Subclasse);
               cmd.Parameters.AddWithValue("@generico", produto.Generico);
               cmd.Parameters.AddWithValue("@lista", produto.Lista);
               cmd.Parameters.AddWithValue("@familia", produto.Familia);
               cmd.Parameters.AddWithValue("@principio_ativo", produto.Principio_ativo);
               cmd.Parameters.AddWithValue("@codigo_guia", produto.Codigo_guia);

               cmd.CommandText = @"
               IF EXISTS (SELECT id_produto FROM Produtos WHERE barras=@barras)
	               UPDATE Produtos SET nome=@nome, apresentacao=@apresentacao, laboratorio=@laboratorio, classe=@classe, 
                  subclasse=@subclasse, generico=@generico, lista=@lista, familia=@familia, principio_ativo=@principio_ativo, codigo_guia=@codigo_guia
                  WHERE barras=@barras
               ELSE
	               INSERT INTO Produtos (barras, nome, apresentacao, laboratorio, classe, subclasse, generico, lista, familia, principio_ativo, codigo_guia)
                  VALUES (@barras, @nome, @apresentacao, @laboratorio, @classe, @subclasse, @generico, @lista, @familia, @principio_ativo, @codigo_guia);";               
               try
               {
                  cmd.ExecuteNonQuery();
               }
               catch (Exception ex)
               {
                  throw new Exception(ex.Message + " | Dados:" + produto.ToString());
               }
               cmd.Parameters.Clear();
            }

            trans.Commit();
         }
         catch (Exception ex)
         {
            trans.Rollback();
            throw new Exception("Erro ao inserir produtos no destino: " + ex.Message);
         }
         finally
         {
            conn.Close();
         }
      }
   }
}
