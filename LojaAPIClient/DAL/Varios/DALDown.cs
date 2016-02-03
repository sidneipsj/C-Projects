using System;
using SQLHelperv2;
using System.Data;

namespace DAL
{   
   public class DALDown
   {   
      #region WebEmpresas, WebEstabelecimentos
      //Alterado para SqlServer
      public static DataTable GetArquivos(string tipo, int id)
      {
         string sql = "SELECT arq_site.arq_id, arq_site.nome, arq_site.descricao, arq_site.dtpublicado";
         sql += " FROM Arq_site";
         sql += " JOIN Arq_empcred ON arq_site.arq_id = arq_empcred.arq_id";
         sql += " AND COALESCE(arq_empcred.apagado,'N')='N'";
         sql += " AND arq_empcred.tipo = '" + tipo + "'";
         sql += " AND arq_empcred.id =" + id;
         sql += " WHERE COALESCE(arq_site.apagado,'N')='N'";
         sql += " ORDER BY arq_id DESC";

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }
      
      //Alterado para SqlServer
      public static SafeDataReader GetArquivo(string tipo, int id, string arquivo)
      {
         string sql = "sELECT arq_site.nome, arq_site.arquivo, arq_site.tamanho";
         sql += " FROM Arq_site";
         sql += " JOIN Arq_empcred ON arq_site.arq_id = arq_empcred.arq_id";
         sql += " AND COALESCE(arq_empcred.apagado,'N')='N'";
         sql += " AND Arq_empcred.tipo = '" + tipo + "'";
         sql += " AND Arq_empcred.id =" + id;
         sql += " WHERE COALESCE(arq_site.apagado,'N')='N'";
         sql += " AND arq_site.arq_id =" + arquivo;

         BD BD = new BD();
         return BD.GetDataReader(sql, null);
      }
      #endregion

      #region WebEmpresas
      //Alterado para SqlServer
      public static DataTable GetAnosFaturas(int empres_id)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("empres_id", empres_id));

         string sql = " SELECT DISTINCT YEAR(data_fatura) AS ano";
         sql += " FROM Fatura f";
         sql += " WHERE f.tipo = 'E' AND f.id = @empres_id";
         sql += " ORDER BY ano DESC";

         BD BD = new BD();
         return BD.GetDataTable(sql,ps);
      }

      //Alterado para SqlServer
      public static DataTable GetFaturas(int empres_id, int ano)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("empres_id", empres_id));
         ps.Add(new Fields("ano", ano));

         string sql = "SELECT fat.id, fat.data_fatura, COALESCE(hora_fatura,'00:00:00') AS hora_fatura,";
         sql += " fat.data_vencimento, fat.so_confirmadas, fat.obs, fat.fatura_id, fat.fechamento,";
         sql += " fat.operador, fat.valor, fat.data_baixa, fat.apagado, fat.baixada,";
         sql += " COALESCE(fat.tipo,'E') AS tipo, CASE WHEN COALESCE(fat.tipo,'E') = 'E' THEN emp.nome";
         sql += " ELSE conv.titular END AS nome, COALESCE(desc_empresa,0) AS desc_empresa, (valor - coalesce(desc_empresa,0)) AS valor_liquido";
         sql += " FROM Fatura fat";
         sql += " LEFT JOIN Empresas emp ON COALESCE(fat.tipo,'E') = 'E' AND emp.empres_id = fat.id";
         sql += " LEFT JOIN Conveniados conv ON fat.tipo = 'C' AND conv.conv_id = fat.id";
         sql += " WHERE COALESCE(fat.apagado,'N') <> 'S'";
         sql += " AND emp.empres_id = @empres_id";
         sql += " AND YEAR(data_fatura) = @ano";
         sql += " ORDER BY data_vencimento DESC";

         BD BD = new BD();
         return BD.GetDataTable(sql, ps);
      }

      public static DataTable GetFatura(string fatura_id)
      {
         string sql = "SELECT conv.conv_id, conv.chapa, conv.titular, sum(debito-credito) as valor_total";
         sql += " FROM Conveniados conv";
         sql += " JOIN Contacorrente cc ON cc.conv_id = conv.conv_id";
         sql += " WHERE cc.fatura_id =" + fatura_id;
         sql += " GROUP BY conv.conv_id, conv.chapa, conv.titular order by conv.titular";

         BD BD = new BD();
         return BD.GetDataTable(sql, null);   
      }

      public static DataRow GetDadosFatura(string fatura_id, string tipo)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("fatura_id", fatura_id));
         ps.Add(new Fields("tipo", tipo));

         string sql = "SELECT fat.data_fatura, fat.id,";
         sql += " fat.data_vencimento,";
         sql += " (fat.valor - coalesce(fat.desc_empresa,0)) as valor";
         sql += " FROM fatura fat";
         sql += " WHERE fat.fatura_id = @fatura_id";
         sql += " AND fat.tipo = @tipo";

         BD BD = new BD();
         return BD.GetOneRow(sql, ps);
      }

      public static DataRow GetDadosBoleto()
      {
         string sql = "SELECT cod_banco, agencia, contacorrente, protestar, dias_protesto,";
         sql += " mensagem1_boleto, mensagem2_boleto, perc_juros, perc_multa, perc_desc,";
         sql += " nome_convenio, cnpj, endereco, numero, complemento, cidade, cep, uf, cod_conv, carteira";
         sql += " FROM Contas_Bancarias WHERE conta_id IN (SELECT conta_id FROM Administradora WHERE apagado <> 'S')";

         BD BD = new BD();
         return BD.GetOneRow(sql, null);
      }

      public static DataTable GetAnosFechamentos(int empres_id)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("empres_id", empres_id));
         ps.Add(new Fields("dataAtual", DateTime.Now));

         string sql = " SELECT DISTINCT EXTRACT(YEAR FROM data_fecha_emp) AS ano";
         sql += " FROM Conveniados conv";
         sql += " JOIN ContaCorrente cc ON cc.conv_id = conv.conv_id";
         sql += " WHERE conv.empres_id = @empres_id";
         sql += " AND cc.data_fecha_emp <= @dataAtual";
         sql += " ORDER BY ano DESC";

         BD BD = new BD();
         return BD.GetDataTable(sql, ps);
      }

      public static DataTable GetFechamentos(int empres_id,int ano)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("empres_id", empres_id));
         ps.Add(new Fields("ano", ano));
         ps.Add(new Fields("dataAtual", DateTime.Now));

         string sql = " SELECT cc.data_fecha_emp, cc.data_venc_emp,";
         sql += " COALESCE(SUM(cc.debito - cc.credito),0) AS valor";
         sql += " FROM Conveniados conv";
         sql += " JOIN Contacorrente cc ON (conv.conv_id = cc.conv_id)";
         sql += " WHERE cc.data_fecha_emp <= @dataAtual";
         sql += " AND EXTRACT(YEAR FROM data_fecha_emp) = @ano";
         sql += " AND conv.apagado <> 'S'";
         sql += " AND conv.empres_id = @empres_id";
         sql += " GROUP BY cc.data_fecha_emp, cc.data_venc_emp";
         sql += " ORDER BY cc.data_fecha_emp DESC";

         BD BD = new BD();
         return BD.GetDataTable(sql, ps);
      }
      
      public static DataTable GetFechamento(int empres_id, DateTime data_fecha)
      {
         string sql = " SELECT conv.conv_id, conv.chapa, conv.titular, ";
         sql += " COALESCE(SUM(cc.debito - cc.credito),0) AS valor ";
         sql += " FROM Conveniados conv  ";
         sql += " JOIN Contacorrente cc ON (conv.conv_id = cc.conv_id) ";
         sql += " WHERE cc.data_fecha_emp = '" + data_fecha.ToString("MM/dd/yyyy") + "' ";
         sql += " AND conv.apagado <> 'S' ";
         sql += " AND conv.empres_id = " + empres_id;
         sql += " GROUP BY conv_id, chapa, titular ";
         sql += " HAVING SUM(cc.debito - cc.credito) > 0";
         sql += " ORDER BY titular";

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }
     
      #endregion
   }
}
