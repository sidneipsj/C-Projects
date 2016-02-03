using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Text;
using SQLHelperv2;
using Negocio;

namespace DAL
{
   public static class DALVendas
   {
      private static List<Vendas> ObterVendas(Redes rede)
      {   
         string sql = @"
         SELECT 
            t.datahora,
            pt.prod_trans_id, pt.codbarras, pt.qtd_aprov, pt.prc_unit_env, pt.vlr_bru, pt.vlr_desc, pt.vale_utilizado, pt.vlr_liq,
            ppt.num_prescritor, ppt.uf_prescritor, ppt.tipo_prescritor, ppt.numreceita, ppt.datareceita,
            COALESCE(prod.nome,prod.descricao) AS prod_nome, prod.apres, prod.generico, prod.lista, prod.cod_guia,
            lab.nomefant,
            clas.classe,
            sclas.subclasse,
            fam.familia,
            pat.pativo,
            cred.cgc, cred.nome AS cred_nome, cred.cidade, cred.estado, cred.cred_id,
            conv.titular, conv.cpf, conv.conv_id
         FROM Transacoes t
         JOIN Prod_trans pt ON pt.trans_id = t.trans_id
         LEFT JOIN Pres_Prod_Trans ppt ON ppt.prod_trans_id = pt.prod_trans_id AND ppt.trans_id = t.trans_id
         JOIN Barras bar ON bar.barras = pt.codbarras
         JOIN Produtos prod ON prod.prod_id = bar.prod_id
         LEFT JOIN Laboratorios lab ON lab.lab_id = prod.lab_id
         LEFT JOIN Classe clas ON clas.clas_id = prod.clas_id
         LEFT JOIN Subclasse sclas ON sclas.sclas_id = prod.sclas_id
         LEFT JOIN Familias fam ON fam.fam_id = prod.fam_id
         LEFT JOIN Pativo pat ON pat.pat_id = prod.pat_id
         JOIN Credenciados cred ON cred.cred_id = t.cred_id
         JOIN Cartoes cart ON cart.cartao_id = t.cartao_id
         JOIN Conveniados conv ON conv.conv_id = cart.conv_id
         WHERE t.confirmada = 'S'
         AND t.cancelado <> 'S'
         AND (t.datahora > @ultima_comunicacao OR t.dtconfirmada > @ultima_comunicacao)
         ORDER BY t.datahora DESC";

         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("ultima_comunicacao", rede.Ultima_comunicacao));

         BD BD = new BD(rede.Tipobanco, rede.Host, rede.Database, rede.User, rede.Pass);

         try
         {
            DataTable table = BD.GetDataTable(sql, ps);

            List<Vendas> vendas = new List<Vendas>();

            foreach (DataRow row in table.Rows)
            {
               Vendas venda = new Vendas();

               DateTime data = Convert.ToDateTime(row["datahora"]);
               venda.Tempo.Data = data;
               venda.Tempo.Ano = data.Year;
               venda.Tempo.Mes = data.Month;
               venda.Tempo.Dia = data.Day;
               venda.Tempo.Semestre = venda.Tempo.Mes < 7 ? "Primeiro" : "Segundo";
               if (venda.Tempo.Mes <= 3)
                  venda.Tempo.Trimestre = "Primeiro";
               else if (venda.Tempo.Mes <= 6)
                  venda.Tempo.Trimestre = "Segundo";
               else if (venda.Tempo.Mes <= 9)
                  venda.Tempo.Trimestre = "Terceiro";
               else
                  venda.Tempo.Trimestre = "Quarto";
               switch (data.DayOfWeek)
               {
                  case DayOfWeek.Friday: venda.Tempo.Dia_semana = "Sexta";
                     break;
                  case DayOfWeek.Monday: venda.Tempo.Dia_semana = "Segunda";
                     break;
                  case DayOfWeek.Saturday: venda.Tempo.Dia_semana = "Sabado";
                     break;
                  case DayOfWeek.Sunday: venda.Tempo.Dia_semana = "Domingo";
                     break;
                  case DayOfWeek.Thursday: venda.Tempo.Dia_semana = "Quinta";
                     break;
                  case DayOfWeek.Tuesday: venda.Tempo.Dia_semana = "Terça";
                     break;
                  case DayOfWeek.Wednesday: venda.Tempo.Dia_semana = "Quarta";
                     break;
               }

               venda.Estab.Cnpj = Misc.Utils.SoNumeros(row["cgc"].ToString());
               if (!Misc.Utils.ValidarCNPJ(venda.Estab.Cnpj)) venda.Estab.Cnpj = "";
               venda.Estab.Razao_social = row["cred_nome"].ToString();
               venda.Estab.Cidade = row["cidade"].ToString();
               venda.Estab.Uf = row["estado"].ToString();
               venda.Estab.Id_estab_rede = Convert.ToInt32(row["cred_id"]);

               venda.Conv.Nome = row["titular"].ToString();
               venda.Conv.Cpf = Misc.Utils.SoNumeros(row["cpf"].ToString());
               if (!Misc.Utils.ValidarCPF(venda.Conv.Cpf)) venda.Conv.Cpf = "";
               venda.Conv.Id_conv_rede = Convert.ToInt32(row["conv_id"]);

               venda.Produto.Barras = row["codbarras"].ToString();
               venda.Produto.Nome = row["prod_nome"].ToString();
               venda.Produto.Apresentacao = row["apres"].ToString();
               venda.Produto.Laboratorio = row["nomefant"].ToString();
               venda.Produto.Classe = row["classe"].ToString();
               venda.Produto.Subclasse = row["subclasse"].ToString();
               venda.Produto.Generico = row["generico"].ToString();
               venda.Produto.Lista = row["lista"].ToString();
               venda.Produto.Familia = row["familia"].ToString();
               venda.Produto.Principio_ativo = row["pativo"].ToString();
               if (!string.IsNullOrEmpty(row["cod_guia"].ToString()))
                  venda.Produto.Codigo_guia = Convert.ToInt32(row["cod_guia"]);

               venda.Quantidade = Convert.ToInt32(row["qtd_aprov"]);
               venda.Valor_unitario = float.Parse(row["prc_unit_env"].ToString());
               venda.Valor_bruto = float.Parse(row["vlr_bru"].ToString());
               venda.Valor_desconto = float.Parse(row["vlr_desc"].ToString());
               venda.Vale_desconto = float.Parse(row["vale_utilizado"].ToString());
               venda.Valor_liquido = float.Parse(row["vlr_liq"].ToString());

               venda.Num_prescritor = "";
               venda.Uf_prescritor = "";
               venda.Tipo_prescritor = "";               
               if (!string.IsNullOrEmpty(row["uf_prescritor"].ToString()))
               {
                  venda.Num_prescritor = row["num_prescritor"].ToString();
                  venda.Uf_prescritor = row["uf_prescritor"].ToString();
                  venda.Tipo_prescritor = row["tipo_prescritor"].ToString();

                  if (!string.IsNullOrEmpty(row["numreceita"].ToString()))
                     venda.Num_receita = Convert.ToInt32(row["numreceita"]);
                  if (!string.IsNullOrEmpty(row["datareceita"].ToString()))
                     venda.Data_receita = Convert.ToDateTime(row["datareceita"]);
               }

               venda.Id_vendaprod_rede = Convert.ToInt32(row["prod_trans_id"]);

               vendas.Add(venda);
            }

            return vendas;
         }
         catch(Exception ex)
         {
            throw new Exception("Erro ao obter vendas da rede " + rede.Nome + ": " + ex.Message);
         }         
      }

      private static List<Vendas> ObterCancelamentos(Redes rede)
      {
         string sql = @"
         SELECT pt.prod_trans_id
         FROM Transacoes t
         JOIN Prod_Trans pt ON pt.trans_id = t.trans_id         
         WHERE t.confirmada = 'S'
         AND t.cancelado = 'S'
         AND (t.dtcancelado > @ultima_comunicacao)";

         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("ultima_comunicacao", rede.Ultima_comunicacao));

         BD BD = new BD(rede.Tipobanco, rede.Host, rede.Database, rede.User, rede.Pass);

         try
         {
            DataTable table = BD.GetDataTable(sql, ps);

            List<Vendas> vendas = new List<Vendas>();

            foreach (DataRow row in table.Rows)
            {
               Vendas venda = new Vendas();
               venda.Id_vendaprod_rede = Convert.ToInt32(row["prod_trans_id"]);
               vendas.Add(venda);
            }

            return vendas;
         }
         catch (Exception ex)
         {
            throw new Exception("Erro ao obter cancelamentos da rede " + rede.Nome + ": " + ex.Message);
         }
      }

      public static void Comunicar(Redes rede)
      {
         string host = System.Configuration.ConfigurationSettings.AppSettings["host"];
         string database = System.Configuration.ConfigurationSettings.AppSettings["database"];
         string user = System.Configuration.ConfigurationSettings.AppSettings["user"];
         string pass = System.Configuration.ConfigurationSettings.AppSettings["pass"];

         SqlConnectionStringBuilder str = new SqlConnectionStringBuilder();
         str.DataSource = host;
         str.InitialCatalog = database;
         if (string.IsNullOrEmpty(user))
            str.IntegratedSecurity = true;
         else
         {
            str.IntegratedSecurity = false;
            str.UserID = user;
            str.Password = pass;
         }
         
         SqlConnection conn = new SqlConnection(str.ToString());
         conn.Open();            
         SqlTransaction trans = conn.BeginTransaction();
         SqlCommand cmd = new SqlCommand();
         cmd.Transaction = trans;
         cmd.Connection = conn;
         
         try
         {
            List<Vendas> vendas = ObterVendas(rede);
            if (vendas.Count > 0)
            {
               foreach (Vendas venda in vendas)
               {
                  #region Tempo
                  cmd.Parameters.AddWithValue("@data", venda.Tempo.Data.Date);
                  cmd.Parameters.AddWithValue("@ano", venda.Tempo.Ano);
                  cmd.Parameters.AddWithValue("@mes", venda.Tempo.Mes);
                  cmd.Parameters.AddWithValue("@dia", venda.Tempo.Dia);
                  cmd.Parameters.AddWithValue("@semestre", venda.Tempo.Semestre);
                  cmd.Parameters.AddWithValue("@trimestre", venda.Tempo.Trimestre);
                  cmd.Parameters.AddWithValue("@dia_semana", venda.Tempo.Dia_semana);
                  cmd.CommandText = @"
               IF EXISTS (SELECT id_tempo FROM Tempo WHERE data=@data)
	               SELECT id_tempo FROM Tempo WHERE data=@data
               ELSE
	               BEGIN
		               INSERT INTO Tempo (data, ano, mes, dia, semestre, trimestre, dia_semana)
                     VALUES (@data, @ano, @mes, @dia, @semestre, @trimestre, @dia_semana);
		               SELECT SCOPE_IDENTITY();
	               END";
                  try
                  {
                     venda.Tempo.Id_tempo = Convert.ToInt32(cmd.ExecuteScalar());
                  }
                  catch (Exception ex)
                  {
                     throw new Exception(ex.Message + " | TEMPO | Dados:" + venda.Id_vendaprod_rede.ToString());
                  }
                  cmd.Parameters.Clear();
                  #endregion

                  #region Estabelecimento
                  cmd.Parameters.AddWithValue("@cnpj", venda.Estab.Cnpj);
                  cmd.Parameters.AddWithValue("@razao_social", venda.Estab.Razao_social);
                  cmd.Parameters.AddWithValue("@cidade", venda.Estab.Cidade);
                  cmd.Parameters.AddWithValue("@uf", venda.Estab.Uf);
                  cmd.Parameters.AddWithValue("@id_rede", rede.Id_rede);
                  cmd.Parameters.AddWithValue("@id_estab_rede", venda.Estab.Id_estab_rede);
                  cmd.CommandText = @"
               IF EXISTS (SELECT id_estab FROM Estabelecimentos WHERE id_rede=@id_rede AND id_estab_rede=@id_estab_rede)
	               SELECT id_estab FROM Estabelecimentos WHERE id_rede=@id_rede AND id_estab_rede=@id_estab_rede
               ELSE
	               BEGIN
		               INSERT INTO Estabelecimentos (cnpj, razao_social, cidade, uf, id_rede, id_estab_rede)
                     VALUES (@cnpj, @razao_social, @cidade, @uf, @id_rede, @id_estab_rede);
		               SELECT SCOPE_IDENTITY();
	               END";
                  try
                  {
                     venda.Estab.Id_estab = Convert.ToInt32(cmd.ExecuteScalar());
                  }
                  catch (Exception ex)
                  {
                     throw new Exception(ex.Message + " | ESTABELECIMENTOS | Dados:" + venda.Id_vendaprod_rede.ToString());
                  }
                  cmd.Parameters.Clear();
                  #endregion

                  #region Conveniados
                  cmd.Parameters.AddWithValue("@nome", venda.Conv.Nome);
                  cmd.Parameters.AddWithValue("@cpf", venda.Conv.Cpf);
                  cmd.Parameters.AddWithValue("@id_rede", rede.Id_rede);
                  cmd.Parameters.AddWithValue("@id_conv_rede", venda.Conv.Id_conv_rede);
                  cmd.CommandText = @"
               IF EXISTS (SELECT id_conv FROM Conveniados WHERE id_rede=@id_rede AND id_conv_rede=@id_conv_rede)
	               SELECT id_conv FROM Conveniados WHERE id_rede=@id_rede AND id_conv_rede=@id_conv_rede
               ELSE
	               BEGIN
		               INSERT INTO Conveniados (nome, cpf, id_rede, id_conv_rede)
                     VALUES (@nome, @cpf, @id_rede, @id_conv_rede);
		               SELECT SCOPE_IDENTITY();
	               END";
                  try
                  {
                     venda.Conv.Id_conv = Convert.ToInt32(cmd.ExecuteScalar());
                  }
                  catch (Exception ex)
                  {
                     throw new Exception(ex.Message + " | CONVENIADOS | Dados:" + venda.Id_vendaprod_rede.ToString());
                  }
                  cmd.Parameters.Clear();
                  #endregion

                  #region Produtos
                  cmd.Parameters.AddWithValue("@barras", venda.Produto.Barras);
                  cmd.Parameters.AddWithValue("@nome", venda.Produto.Nome);
                  cmd.Parameters.AddWithValue("@apresentacao", venda.Produto.Apresentacao);
                  cmd.Parameters.AddWithValue("@laboratorio", venda.Produto.Laboratorio);
                  cmd.Parameters.AddWithValue("@classe", venda.Produto.Classe);
                  cmd.Parameters.AddWithValue("@subclasse", venda.Produto.Subclasse);
                  cmd.Parameters.AddWithValue("@generico", venda.Produto.Generico);
                  cmd.Parameters.AddWithValue("@lista", venda.Produto.Lista);
                  cmd.Parameters.AddWithValue("@familia", venda.Produto.Familia);
                  cmd.Parameters.AddWithValue("@principio_ativo", venda.Produto.Principio_ativo);
                  if (venda.Produto.Codigo_guia > 0) cmd.Parameters.AddWithValue("@codigo_guia", venda.Produto.Codigo_guia);

                  cmd.CommandText = @"
               IF EXISTS (SELECT id_produto FROM Produtos WHERE barras=@barras)
	               SELECT id_produto FROM Produtos WHERE barras=@barras
               ELSE
	               BEGIN
		               INSERT INTO Produtos (barras, nome, apresentacao, laboratorio, classe, subclasse, generico, lista, familia, principio_ativo";
                  if (venda.Produto.Codigo_guia > 0)
                     cmd.CommandText += ",codigo_guia";
                  cmd.CommandText += ") VALUES (@barras, @nome, @apresentacao, @laboratorio, @classe, @subclasse, @generico, @lista, @familia, @principio_ativo";
                  if (venda.Produto.Codigo_guia > 0)
                     cmd.CommandText += ",@codigo_guia";
                  cmd.CommandText += "); SELECT SCOPE_IDENTITY(); END";
                  try
                  {
                     venda.Produto.Id_produto = Convert.ToInt32(cmd.ExecuteScalar());
                  }
                  catch (Exception ex)
                  {
                     throw new Exception(ex.Message + " | PRODUTOS | Dados:" + venda.Id_vendaprod_rede.ToString());
                  }
                  cmd.Parameters.Clear();
                  #endregion

                  #region Venda
                  cmd.Parameters.AddWithValue("@id_tempo", venda.Tempo.Id_tempo);
                  cmd.Parameters.AddWithValue("@id_rede", rede.Id_rede);
                  cmd.Parameters.AddWithValue("@id_estab", venda.Estab.Id_estab);
                  cmd.Parameters.AddWithValue("@id_conv", venda.Conv.Id_conv);
                  cmd.Parameters.AddWithValue("@id_produto", venda.Produto.Id_produto);
                  cmd.Parameters.AddWithValue("@quantidade", venda.Quantidade);
                  cmd.Parameters.AddWithValue("@valor_unitario", Misc.Utils.decimalSql(venda.Valor_unitario));
                  cmd.Parameters.AddWithValue("@valor_bruto", Misc.Utils.decimalSql(venda.Valor_bruto));
                  cmd.Parameters.AddWithValue("@valor_desconto", Misc.Utils.decimalSql(venda.Valor_desconto));
                  cmd.Parameters.AddWithValue("@vale_desconto", Misc.Utils.decimalSql(venda.Vale_desconto));
                  cmd.Parameters.AddWithValue("@valor_liquido", Misc.Utils.decimalSql(venda.Valor_liquido));
                  cmd.Parameters.AddWithValue("@pres_num", venda.Num_prescritor);
                  cmd.Parameters.AddWithValue("@pres_uf", venda.Uf_prescritor);
                  cmd.Parameters.AddWithValue("@pres_tipo", venda.Tipo_prescritor);
                  cmd.Parameters.AddWithValue("@id_vendaprod_rede", venda.Id_vendaprod_rede);
                  if (venda.Num_receita > 0) cmd.Parameters.AddWithValue("@num_receita", venda.Num_receita);
                  if (venda.Data_receita != DateTime.MinValue) cmd.Parameters.AddWithValue("@data_receita", venda.Data_receita);
                  cmd.CommandText = @"
               IF NOT EXISTS (SELECT id_vendaprod_rede FROM Vendas WHERE id_rede=@id_rede AND id_vendaprod_rede=@id_vendaprod_rede)
	            BEGIN
		            INSERT INTO Vendas (id_tempo, id_rede, id_estab, id_conv, id_produto, quantidade,                  
                  valor_unitario, valor_bruto, valor_desconto, vale_desconto, valor_liquido,
                  num_prescritor, uf_prescritor, tipo_prescritor, id_vendaprod_rede";
                  if (venda.Num_receita > 0)
                     cmd.CommandText += ",num_receita";
                  if (venda.Data_receita != DateTime.MinValue)
                     cmd.CommandText += ", data_receita";
                  cmd.CommandText += @") VALUES (@id_tempo, @id_rede, @id_estab, @id_conv, @id_produto, @quantidade,                  
                  @valor_unitario, @valor_bruto, @valor_desconto, @vale_desconto, @valor_liquido,
                  @pres_num, @pres_uf, @pres_tipo, @id_vendaprod_rede";
                  if (venda.Num_receita > 0)
                     cmd.CommandText += ",@num_receita";
                  if (venda.Data_receita != DateTime.MinValue)
                     cmd.CommandText += ", @data_receita";
                  cmd.CommandText += "); END";
                  try
                  {
                     cmd.ExecuteNonQuery();
                  }
                  catch (Exception ex)
                  {
                     throw new Exception(ex.Message + " | VENDA | Dados:" + venda.Id_vendaprod_rede.ToString());
                  }
                  cmd.Parameters.Clear();
                  #endregion
               }

               cmd.Parameters.AddWithValue("@id_rede", rede.Id_rede);
               cmd.Parameters.AddWithValue("@ultima_comunicacao", vendas[0].Tempo.Data);
               cmd.CommandText = "UPDATE Redes SET ultima_comunicacao= @ultima_comunicacao WHERE id_rede=@id_rede";
               try
               {
                  cmd.ExecuteNonQuery();
               }
               catch (Exception ex)
               {
                  throw new Exception(ex.Message + " | ULTIMA COMUNICACAO");
               }
               cmd.Parameters.Clear();
            }

            List<Vendas> cancelamentos = ObterCancelamentos(rede);
            foreach (Vendas cancelamento in cancelamentos)
            {
               cmd.Parameters.AddWithValue("@id_rede", rede.Id_rede);
               cmd.Parameters.AddWithValue("@id_vendaprod_rede", cancelamento.Id_vendaprod_rede);
               cmd.CommandText = "DELETE FROM Vendas WHERE id_rede=@id_rede AND id_vendaprod_rede = @id_vendaprod_rede";
               try
               {
                  cmd.ExecuteNonQuery();
               }
               catch (Exception ex)
               {
                  throw new Exception(ex.Message + " | CANCELAMENTO | Dados: " + cancelamento.Id_vendaprod_rede.ToString());
               }
               cmd.Parameters.Clear();
            }

            trans.Commit();
         }
         catch (Exception ex)
         {
            trans.Rollback();
            throw new Exception("Erro ao comunicar rede " + rede.Nome + ": " + ex.Message);
         }
         finally
         {
            conn.Close();
         }
      }
   }
}
