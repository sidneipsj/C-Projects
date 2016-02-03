using System;
using System.Data;
using SQLHelperv2;
using System.Data.SqlClient;

namespace DAL
{
    public class DALOndeComprar
    {
        public static DataTable GetCidades()
        {
            string sql = "SELECT DISTINCT cidade FROM Credenciados WHERE liberado = 'S' ORDER BY cidade";
            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable GeSegmentos()
        {
            string sql = "SELECT * FROM Segmentos ORDER BY descricao";
            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable ocGetSegmentos()
        {
            string sql = "SELECT DISTINCT descricao, seg_id from segmentos WHERE seg_id<>0 and APAGADO<>'S' order by descricao";
            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable ocGetcidadeCredenciados(int idSegmento = 0)
        {
            string sql;
            if (idSegmento == 0)
            {
                sql = "SELECT DISTINCT CIDADE FROM CREDENCIADOS WHERE apagado = 'N' and liberado = 'S' AND CIDADE <> '' order by cidade;";
            }
            else
            {
                sql = "select DISTINCT c.cidade, c.seg_id from credenciados AS c WHERE cidade != '' and c.seg_id = " + idSegmento + " AND apagado = 'N' and liberado = 'S' order by cidade ;";
            }
            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable ocGetBairroCredenciados(int idSegmento, bool setouCidade, String cidade = "")// a variável set setouCidade está verificando se o usuário setou cidade ou não
        {
            string sql;
            if (idSegmento == 0)
            {
                if (setouCidade == true)
                {
                    sql = "select distinct bairro from credenciados where cidade like'%" + cidade + "%' AND apagado = 'N' and liberado = 'S' order by bairro";
                }
                else
                {
                    sql = "SELECT cred_id, (CASE WHEN (fantasia = '' OR fantasia IS NULL) THEN nome ELSE fantasia END) AS fantasia FROM Credenciados WHERE apagado = 'N' and liberado = 'S' order by bairro";
                }
            }
            else
            {
                if (setouCidade == true)
                {
                    sql = "select distinct c.bairro from credenciados as c join segmentos as s on c.seg_id = " + idSegmento + " and cidade like'%" + cidade + "%' AND c.apagado = 'N' and c.liberado = 'S' order by bairro";
                }
                else
                {
                    sql = "SELECT cred_id, (CASE WHEN (fantasia = '' OR fantasia IS NULL) THEN nome ELSE fantasia END) AS fantasia FROM Credenciados WHERE apagado = 'N' and liberado = 'S' and seg_id = " + idSegmento + " order by bairro;";
                }
            }
            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable ocFiltroPorBairro(int segId, bool setouBairro, string cidade, string bairro = "")
        {
            string sql;
            if (segId != 0)
            {
                if (setouBairro == true)
                {
                    sql = "SELECT fantasia, descricao, endereco, numero, bairro, cidade, estado, telefone1 FROM Credenciados, segmentos WHERE Credenciados.apagado = 'N' and Credenciados.liberado = 'S' AND segmentos.seg_id = " + segId + " and cidade = '" + cidade + "' and bairro = '" + bairro + "' and segmentos.seg_id = credenciados.seg_id order by descricao, fantasia";
                }
                else
                {
                    sql = "SELECT fantasia, descricao, endereco, numero, bairro, cidade, estado, telefone1 FROM Credenciados, segmentos WHERE Credenciados.apagado = 'N' and Credenciados.liberado = 'S' AND segmentos.seg_id = " + segId + " and cidade = '" + cidade + "' and segmentos.seg_id = credenciados.seg_id order by descricao, fantasia";
                }
            }
            else
            {
                if (setouBairro == true)
                {
                    sql = "SELECT fantasia, descricao, endereco, numero, bairro, cidade, estado, telefone1 FROM Credenciados, segmentos Where cidade = '" + cidade + "' AND bairro = '" + bairro + "' AND Credenciados.apagado = 'N' and Credenciados.liberado = 'S' and segmentos.seg_id = credenciados.seg_id order by descricao, fantasia";
                }
                else
                {
                    sql = "SELECT fantasia, descricao, endereco, numero, bairro, cidade, estado, telefone1 FROM Credenciados, segmentos Where cidade = '" + cidade + "' AND Credenciados.apagado = 'N' and Credenciados.liberado = 'S' and segmentos.seg_id = credenciados.seg_id order by descricao, fantasia";
                }
            }

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable ocListaDadosCredenciados(int id_segmento)
        {
            string sql;
            if (id_segmento == 0)
            {
                //sql = "SELECT * FROM credenciados WHERE APAGADO = 'N' AND LIBERADO = 'S'";
                sql = "SELECT fantasia, descricao, endereco, numero, bairro, cidade, estado, telefone1 FROM credenciados, segmentos WHERE Credenciados.liberado = 'S' AND Credenciados.apagado = 'N' and segmentos.seg_id = credenciados.seg_id order by descricao, fantasia";
            }
            else
            {
                //sql = "SELECT nome, fantasia, endereco, numero, bairro, cidade, estado, telefone1 FROM credenciados WHERE liberado = 'S' AND apagado = 'N'AND seg_id = " + id_segmento + ";";
                sql = "SELECT fantasia, descricao, endereco, numero, bairro, cidade, estado, telefone1 FROM credenciados, segmentos WHERE Credenciados.liberado = 'S' AND Credenciados.apagado = 'N' AND segmentos.seg_id = " + id_segmento + " and segmentos.seg_id = credenciados.seg_id order by descricao, fantasia";
            }
            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }
        public static int CountCredenciados(bool pesqSeg, string cidade, string segmento, string fantasia)
        {
            string sql = "SELECT COUNT (*) FROM Credenciados ";

            if (pesqSeg)
            {
                sql += " WHERE liberado = 'S'";
                if (cidade != "")
                    sql += " AND cidade = '" + cidade + "'";
                sql += " AND seg_id = " + segmento;
            }
            else
                sql += " WHERE liberado = 'S' AND  fantasia LIKE '%" + fantasia.ToUpper() + "%'";

            BD BD = new BD();
            DataTable tabela = BD.GetDataTable(sql, null);

            return Convert.ToInt32(tabela.Rows[0][0].ToString());
        }

        public static DataTable GetCredenciados(int page, int start, bool pesqSeg, string cidade, string segmento, string fantasia)
        {
            string sql = "SELECT FIRST " + page + " SKIP " + start + " cred_id, (CASE WHEN (fantasia = '' OR fantasia IS NULL) THEN nome ELSE fantasia END) AS fantasia ";
            sql += "FROM Credenciados ";

            if (pesqSeg)
            {
                sql += "WHERE liberado = 'S'";
                if (cidade != "")
                    sql += " AND cidade = '" + cidade + "'";
                sql += " AND seg_id = " + segmento;
            }
            else
                sql += " WHERE liberado = 'S' AND fantasia LIKE '%" + fantasia.ToUpper() + "%'";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable GetDetalhes(string id)
        {
            string sql = "SELECT bairro, cep, cred_id, cidade, contato, email, endereco, ";
            sql += " estado, fantasia, fax, homepage, nome, telefone1, telefone2 ";
            sql += " FROM Credenciados ";
            sql += " WHERE cred_id = @Id";

            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("Id", id));

            BD BD = new BD();
            return BD.GetDataTable(sql, ps);
        }

        #region BEM ESTAR

        public static DataTable bePesquisaEspecialidade()
        {
            string sql = "SELECT DESCRICAO, ESPECIALIDADE_ID FROM ESPECIALIDADES";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable bePesquisaCidades(int codEspecialidade)
        {
            string sql = "SELECT DISTINCT Upper(CID.NOME) AS CIDADE, CID.CID_ID FROM CRED_ESPEC_BEM_ESTAR CREDEPEC "
                       + "INNER JOIN ESPECIALIDADES ESP ON(ESP.ESPECIALIDADE_ID = CREDEPEC.ESPECIALIDADE_ID) "
                       + "INNER JOIN CREDENCIADOS_BEM_ESTAR CRED ON(CRED.CRED_BE_ID = CREDEPEC.CRED_BE_ID) "                       
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID) "
                       + "WHERE ESP.ESPECIALIDADE_ID =" + codEspecialidade + "";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable bePesquisaEspecialidadesCidades()
        {
            string sql = "SELECT DISTINCT Upper(CID.NOME) AS CIDADE, CID.CID_ID FROM CREDENCIADOS_BEM_ESTAR CRED "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID)";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable bePesquisaBairros(int codCidade, int codEspecialidade)
        {
            string sql = "SELECT DISTINCT CRED.BAIRRO FROM CRED_ESPEC_BEM_ESTAR CREDEPEC "
                       + "INNER JOIN ESPECIALIDADES ESP ON(ESP.ESPECIALIDADE_ID = CREDEPEC.ESPECIALIDADE_ID) "
                       + "INNER JOIN CREDENCIADOS_BEM_ESTAR CRED ON(CRED.CRED_BE_ID = CREDEPEC.CRED_BE_ID) "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID) "
                       + "WHERE CRED.CID_ID = " + codCidade + " AND ESP.ESPECIALIDADE_ID = " + codEspecialidade + "";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable bePesquisaBairrosPorCidades(int codCidade) 
        {
            string sql = "SELECT DISTINCT CRED.BAIRRO FROM CREDENCIADOS_BEM_ESTAR CRED "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID) "
                       + "WHERE CRED.CID_ID = " + codCidade + "";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable bePesquisaTodosMedicos() 
        {
            string sql = "SELECT CRED.FANTASIA, ESP.DESCRICAO AS ESPECIALIDADE, Upper(CID.NOME) AS CIDADE, CRED.BAIRRO, CRED.FONE1, CRED.OBS, CRED.ENDERECO, CRED.NUMERO "
                       + "FROM CRED_ESPEC_BEM_ESTAR CREDEPEC "
                       + "INNER JOIN ESPECIALIDADES ESP ON(ESP.ESPECIALIDADE_ID = CREDEPEC.ESPECIALIDADE_ID) "
                       + "INNER JOIN CREDENCIADOS_BEM_ESTAR CRED ON(CRED.CRED_BE_ID = CREDEPEC.CRED_BE_ID) "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID)";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable bePesquisaTodosMedicosPorCidade(int codCidade) 
        {
            string sql = "SELECT CRED.FANTASIA, ESP.DESCRICAO AS ESPECIALIDADE, Upper(CID.NOME) AS CIDADE, CRED.BAIRRO, CRED.FONE1, CRED.OBS, CRED.ENDERECO, CRED.NUMERO "
                       + "FROM CRED_ESPEC_BEM_ESTAR CREDEPEC "
                       + "INNER JOIN ESPECIALIDADES ESP ON(ESP.ESPECIALIDADE_ID = CREDEPEC.ESPECIALIDADE_ID) "
                       + "INNER JOIN CREDENCIADOS_BEM_ESTAR CRED ON(CRED.CRED_BE_ID = CREDEPEC.CRED_BE_ID) "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID) "
                       + "WHERE CID.CID_ID = " + codCidade + "";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable bePesquisaTodosMedicosPorCidadeEBairro(int codCidade, string bairro)
        {
            string sql = "SELECT CRED.FANTASIA, ESP.DESCRICAO AS ESPECIALIDADE, Upper(CID.NOME) AS CIDADE, CRED.BAIRRO, CRED.FONE1, CRED.OBS, CRED.ENDERECO, CRED.NUMERO "
                       + "FROM CRED_ESPEC_BEM_ESTAR CREDEPEC "
                       + "INNER JOIN ESPECIALIDADES ESP ON(ESP.ESPECIALIDADE_ID = CREDEPEC.ESPECIALIDADE_ID) "
                       + "INNER JOIN CREDENCIADOS_BEM_ESTAR CRED ON(CRED.CRED_BE_ID = CREDEPEC.CRED_BE_ID) "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID) "
                       + "WHERE CID.CID_ID = " + codCidade + " AND CRED.BAIRRO = '" + bairro + "'";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable bePesquisaMedicosPorEspecialidade(int codEspecialidade)
        {
            string sql = "SELECT CRED.FANTASIA, ESP.DESCRICAO AS ESPECIALIDADE, Upper(CID.NOME) AS CIDADE, CRED.BAIRRO, CRED.FONE1, CRED.OBS, CRED.ENDERECO, CRED.NUMERO  "
                       + "FROM CRED_ESPEC_BEM_ESTAR CREDEPEC "
                       + "INNER JOIN ESPECIALIDADES ESP ON(ESP.ESPECIALIDADE_ID = CREDEPEC.ESPECIALIDADE_ID) "
                       + "INNER JOIN CREDENCIADOS_BEM_ESTAR CRED ON(CRED.CRED_BE_ID = CREDEPEC.CRED_BE_ID) "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID) "
                       + "WHERE ESP.ESPECIALIDADE_ID =" + codEspecialidade + "";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable bePesquisaMedicosPorEspecialidadeECidade(int codEspecialidade, int codCidade) 
        {
            string sql = "SELECT CRED.FANTASIA, ESP.DESCRICAO AS ESPECIALIDADE, Upper(CID.NOME) AS CIDADE, CRED.BAIRRO, CRED.FONE1, CRED.OBS, CRED.ENDERECO, CRED.NUMERO "
                       + "FROM CRED_ESPEC_BEM_ESTAR CREDEPEC "
                       + "INNER JOIN ESPECIALIDADES ESP ON(ESP.ESPECIALIDADE_ID = CREDEPEC.ESPECIALIDADE_ID) "
                       + "INNER JOIN CREDENCIADOS_BEM_ESTAR CRED ON(CRED.CRED_BE_ID = CREDEPEC.CRED_BE_ID) "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID) "
                       + "WHERE ESP.ESPECIALIDADE_ID = " + codEspecialidade + " AND CID.CID_ID = " + codCidade + "";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable bePesquisaMedicosPorEspecialidadeCidadeEBarirro(int codEspecialidade, int codCidade, string bairro) 
        {
            string sql = "SELECT CRED.FANTASIA, ESP.DESCRICAO AS ESPECIALIDADE, Upper(CID.NOME) AS CIDADE, CRED.BAIRRO, CRED.FONE1, CRED.OBS, CRED.ENDERECO, CRED.NUMERO "
                       + "FROM CRED_ESPEC_BEM_ESTAR CREDEPEC "
                       + "INNER JOIN ESPECIALIDADES ESP ON(ESP.ESPECIALIDADE_ID = CREDEPEC.ESPECIALIDADE_ID) "
                       + "INNER JOIN CREDENCIADOS_BEM_ESTAR CRED ON(CRED.CRED_BE_ID = CREDEPEC.CRED_BE_ID) "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID) "
                       + "WHERE ESP.ESPECIALIDADE_ID = " + codEspecialidade + " AND CID.CID_ID = " + codCidade + " AND CRED.BAIRRO = '" + bairro + "'";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable bePesquisaTodasEspecialidadesPorCidade() 
        {
            string sql = "SELECT Distinct Upper(CID.NOME), CID_ID FROM ESPECIALIDADES ESP "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = ESP.COD_CIDADE) "
                       + "WHERE CID.CID_ID = COD_CIDADE";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable bePesquisaRapidaMedicoPorNome(string nomeMedico) 
        {
            string sql = "SELECT CRED.FANTASIA, ESP.DESCRICAO AS ESPECIALIDADE, Upper(CID.NOME) AS CIDADE, CRED.BAIRRO, CRED.FONE1, CRED.OBS, CRED.ENDERECO, CRED.NUMERO "
                       + "FROM CRED_ESPEC_BEM_ESTAR CREDEPEC "
                       + "INNER JOIN ESPECIALIDADES ESP ON(ESP.ESPECIALIDADE_ID = CREDEPEC.ESPECIALIDADE_ID) "
                       + "INNER JOIN CREDENCIADOS_BEM_ESTAR CRED ON(CRED.CRED_BE_ID = CREDEPEC.CRED_BE_ID) "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID) "
                       + "WHERE CRED.FANTASIA LIKE '%" + nomeMedico + "%'";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        //QUANTIDADES PARA MONTAR DINAMICAMENTE O LOCALIZAR MEDICOS
        public static string beQuantidadePorEspecialidade(int codEspecialidade)
        {
            string sql = "SELECT COUNT(CRED_BE_ID) AS C from CRED_ESPEC_BEM_ESTAR WHERE ESPECIALIDADE_ID = " + codEspecialidade + "";

            BD BD = new BD();
            return BD.ExecuteScalar(sql, null).ToString();
        }

        public static string beQuantidadeTotal()
        {
            string sql = "SELECT COUNT(CRED_BE_ID) AS C from CRED_ESPEC_BEM_ESTAR";

            BD BD = new BD();
            return BD.ExecuteScalar(sql, null).ToString();
        }

        public static string beQuantidadeTotalPorCidade(int codCidade) 
        {
            string sql = "SELECT COUNT(BE.CRED_BE_ID) AS C from CRED_ESPEC_BEM_ESTAR BE "
                       + "INNER JOIN CREDENCIADOS_BEM_ESTAR CRED ON(CRED.CRED_BE_ID = BE.CRED_BE_ID) "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID) "
                       + "WHERE CID.CID_ID = " + codCidade + "";

            BD BD = new BD();
            return BD.ExecuteScalar(sql, null).ToString();
        }

        public static string beQuantidadeTotalPorCidadeBairro(int codCidade, string bairro)
        {
            string sql = "SELECT COUNT(BE.CRED_BE_ID) AS C from CRED_ESPEC_BEM_ESTAR BE "
                       + "INNER JOIN CREDENCIADOS_BEM_ESTAR CRED ON(CRED.CRED_BE_ID = BE.CRED_BE_ID) "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID) "
                       + "WHERE CID.CID_ID = " + codCidade + " AND CRED.BAIRRO = '" + bairro + "'";

            BD BD = new BD();
            return BD.ExecuteScalar(sql, null).ToString();
        }

        public static string beQuantidadeEspecialidadeCidade(int codEspecialidade, int codCidade)
        {
            string sql = "SELECT COUNT(BE.CRED_BE_ID) AS C from CRED_ESPEC_BEM_ESTAR BE "
                       + "INNER JOIN CREDENCIADOS_BEM_ESTAR CRED ON(CRED.CRED_BE_ID = BE.CRED_BE_ID) "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID) "
                       + "WHERE ESPECIALIDADE_ID = " + codEspecialidade + " AND CID.CID_ID = " + codCidade + "";

            BD BD = new BD();
            return BD.ExecuteScalar(sql, null).ToString();
        }

        public static string beQuantidadeEspecialidadeCidadeBairro(int codEspecialidade, int codCidade, string bairro)
        {
            string sql = "SELECT COUNT(BE.CRED_BE_ID) AS C from CRED_ESPEC_BEM_ESTAR BE "
                       + "INNER JOIN CREDENCIADOS_BEM_ESTAR CRED ON(CRED.CRED_BE_ID = BE.CRED_BE_ID) "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID) "
                       + "WHERE ESPECIALIDADE_ID = " + codEspecialidade + " AND CID.CID_ID = " + codCidade + " AND CRED.BAIRRO = '" + bairro + "'";

            BD BD = new BD();
            return BD.ExecuteScalar(sql, null).ToString();
        }

        public static string beQuantidadePesquisaRapida(string nome)
        {
            string sql = "SELECT COUNT(BE.CRED_BE_ID) AS C from CRED_ESPEC_BEM_ESTAR BE "
                       + "INNER JOIN CREDENCIADOS_BEM_ESTAR CRED ON(CRED.CRED_BE_ID = BE.CRED_BE_ID) "
                       + "INNER JOIN CIDADES CID ON(CID.CID_ID = CRED.CID_ID) "
                       + "WHERE CRED.FANTASIA LIKE '%" + nome + "%'";

            BD BD = new BD();
            return BD.ExecuteScalar(sql, null).ToString();
        }

        #endregion
    }
}