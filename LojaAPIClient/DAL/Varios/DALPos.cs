using System;
using System.Collections.Generic;
using System.Text;
using SQLHelperv2;
using Funcoes;
using System.Data;


namespace DAL
{
  public class DALPos
  {
    /// <summary>
    /// Diz se existe o POS cadastrado no banco;
    /// </summary>
    /// <param name="numSerie">número de série do POS</param>
    /// <returns>o Retorno é true  caso o Terminal/POS exista no BD e false caso contrário</returns>
    //Alterado para SqlServer
    public static bool existeTerminal(string numSerie)
    {
      SqlParamsList ps = new SqlParamsList();
      ps.Add(new Fields("@serial_number", numSerie));
      BD BD = new BD();
      int qtd = Convert.ToInt32(BD.ExecuteScalar(-1, "select count(*) from pos where pos_serial_number = @serial_number", ps));
      return qtd > 0;
    }

    //Alterado para SqlServer
    public static string getRazaoCredPorNumeroDeSerie(string numSerie)
    {
      SqlParamsList ps = new SqlParamsList();
      ps.Add(new Fields("@serial_number", numSerie));
      BD BD = new BD();
      string s = Convert.ToString(BD.ExecuteScalar("", "select (select top 1 substring(nome,1,38) from credenciados c where cred_id = p.cred_id and apagado <> 'S') razao from pos p where p.pos_serial_number = @serial_number", ps));
      return s;
    }

    public static int GetVersaoOS(string numSerie)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@serial_number", numSerie));
        BD BD = new BD();
        string status = Convert.ToString(BD.ExecuteScalar("N", "select versao_os from pos where pos_serial_number = @serial_number", ps));
        return (status == "N" ? 0 : 1);
    }

    public static int GetVersaoEOS(string numSerie)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@serial_number", numSerie));
        BD BD = new BD();
        string status = Convert.ToString(BD.ExecuteScalar("N", "select VERSAO_EOS from pos where pos_serial_number = @serial_number", ps));
        return (status == "N" ? 0 : 1);
    }

    public static int GetAtualizouEOS(string numSerie, string status)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@serial_number", numSerie));
        BD BD = new BD();

        if (BD.ExecuteNoQuery("update pos set VERSAO_EOS = '" + status + "' where pos_serial_number = @serial_number", ps).Equals(1))
        {
            return BD.ExecuteNoQuery("INSERT LOG_POS VALUES ('" + numSerie + "','" + DateTime.Now + "','" + status + "')", null);
        }
        else
            return 0;
    }

    public static int GetAtualizouOS(string numSerie, string status)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@serial_number", numSerie));
        BD BD = new BD();

        if (BD.ExecuteNoQuery("update pos set VERSAO_OS = '" + status + "' where pos_serial_number = @serial_number", ps).Equals(1))
        {
            return BD.ExecuteNoQuery("INSERT LOG_POS VALUES ('" + numSerie + "','" + DateTime.Now + "','" + status + "')", null);
        }
        else
            return 0;
    }

    public static string GetVersaoNavs(string numSerie)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@serial_number", numSerie));
        BD BD = new BD();
        string s = Convert.ToString(BD.ExecuteScalar("", "select p.versao_navs from pos p where p.pos_serial_number = @serial_number", ps));
        return s;
    }

    /// <summary>
    /// Verifica se o POS está aberto
    /// </summary>
    /// <param name="numSerie">qual o número de serie do Terminal/POS?</param>
    /// <returns>retornos 0: indica que ele está fechado; 1: indica que ele está abertocaso; -1: indica que ele não existe</returns>  
    //Alterado para SqlServer
    public static int verificaPosAberto(string numSerie)
    {
      if (existeTerminal(numSerie))
      {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@serial_number", numSerie));
        BD BD = new BD();
        string status = Convert.ToString(BD.ExecuteScalar("N", "select top 1 status from terminais_abertura where pos_serial_number = @serial_number order by data_abertura desc", ps));
        return (status == "N" ? 0 : 1);
      }
      else
      {
        return -1;
      }
    }

    public static int GetAtualizaLua(string numSerie)
    {
        if (existeTerminal(numSerie))
        {
            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("@serial_number", numSerie));
            BD BD = new BD();
            string status = Convert.ToString(BD.ExecuteScalar("N", "select ATUALIZOU_LUA from pos where pos_serial_number = @serial_number", ps));
            return (status == "N" ? 0 : 1);
        }
        else
        {
            return -1;
        }
    }

    public static int GetLogPos(string numSerie)
    {
        BD BD = new BD();

        return BD.ExecuteNoQuery("INSERT LOG_POS VALUES ('" + numSerie + "','" + DateTime.Now + "','S')", null);
    }

    public static int GetAtualizouLua(string numSerie)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@serial_number", numSerie));
        BD BD = new BD();

        if (BD.ExecuteNoQuery("update pos set ATUALIZOU_LUA = 'S' where pos_serial_number = @serial_number", ps).Equals(1))
        {
            return BD.ExecuteNoQuery("INSERT LOG_POS VALUES ('" + numSerie + "','" + DateTime.Now + "','S')", null);
        }
        else
            return 0;
    }


    public static int verificaAutServerIP(string numSerie)
    {
        if (existeTerminal(numSerie))
        {
            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("@serial_number", numSerie));
            BD BD = new BD();
            string status = Convert.ToString(BD.ExecuteScalar("N", "select atu_server_ip from pos where pos_serial_number = @serial_number", ps));
            return (status == "N" ? 0 : 1);
        }
        else
        {
            return -1;
        }
    }



    /// <summary>
    /// Verifica se a senha passada é a mesma do credenciado referente ao número de série do POS passado no parâmetro
    /// </summary>
    /// <param name="senha">senha do credenciado</param>
    /// <param name="numSerie">número de série do POS do credenciado desejado</param>
    /// <returns>retorna True caso a senha seja igual e False caso contrário</returns>
    //Alterado para SqlServer
    public static bool verificaSenhaCredPorPos(string numSerie, string senha)
    {
      SqlParamsList ps = new SqlParamsList();
      ps.Add(new Fields("@numSerie", numSerie));
      BD BD = new BD();
      string r = Convert.ToString(BD.ExecuteScalar("select (select top 1 senha from credenciados where cred_id = p.cred_id and apagado <> 'S') senha from pos p where pos_serial_number = @numSerie", ps));
      Crypt c = new Crypt();
      string crypto = c.Crypt("D", r, "BIGCOMPRAS");
      return crypto.Equals(senha);
    }

    /// <summary>
    /// Captura o CredId referente ao POS cadastrado
    /// </summary>
    /// <param name="numSerie">Número de Série do terminal/POS</param>
    /// <returns>retorna o ID do credenciado</returns>
    //Alterado para SqlServer
    public static int getCredIdPeloSerialNumberPos(string numSerie)
    {
      SqlParamsList ps = new SqlParamsList();
      ps.Add(new Fields("@numSerie", numSerie));
      BD BD = new BD();      
      int r = Convert.ToInt32(BD.ExecuteScalar("select cred_id from pos p where pos_serial_number = @numSerie", ps));
      return r;
    }

    /// <summary>
    /// Abre ou Fecha o Terminal/POS
    /// </summary>
    /// <param name="abrir">deseja abrir ou fechar o terminal/POS?</param>
    /// <param name="numSerie">qual o número de Série do Terminal/POS?</param>
    /// <param name="codacesso">qual o código de acesso do estabelecimento?</param>
    /// <returns>retorna um estado do Termainal/POS sendo: 0 = indicando que incluiu com sucesso; 1 = indicando que o número de serie do terminal/POS não foi encontrado; e 2 = indicando que houve um erro ao tentar incluir no banco 3 = senha invalida 4 = terminal já está aberto 5 = terminal já está fechado 6 = senha incorreta</returns>
    //Alterado para SqlServer
    public static int abrirFecharPOS(bool abrir, string numSerie, string senha)
    {
      if (existeTerminal(numSerie))
      {
        if (verificaSenhaCredPorPos(numSerie, senha))
        {
          SqlParamsList ps = new SqlParamsList();
          ps.Add(new Fields("@numSerie", numSerie));
          BD BD = new BD();
          int r = verificaPosAberto(numSerie);
          if (abrir && r == 1)
            return 4;
          else if (!abrir && r == 0)
            return 5;
          //adicionando novo parâmetro pra incluir o registro como "aberto" ou "fechado"          
          DateTime dt = DateTime.Parse(DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), new System.Globalization.CultureInfo("pt-br"));
          if (incluirTerminaisAbertura(dt, abrir, numSerie))
            return 0;
          else
            return 2;
        }
        else
        {
          return 6;
        }
      }
      else
      {
        return 1;
      }
    }

    public static int getAtualizouServerIP(string numSerie)
    {
      BD BD = new BD();
      return Convert.ToInt32(BD.ExecuteScalar("update pos set atu_server_ip = 'S' where pos_serial_number = '" + numSerie + "'", null));
    }
      

    /// <summary>
    /// inclusão de registro na tabela TERMINAIS_ABERTURA
    /// </summary>
    /// <param name="dataHora">Data</param>
    /// <param name="credId">CRED_ID</param>
    /// <param name="abrir">abrir (S,N)</param>
    /// <param name="serial_number">SERIAL_NUMBER (da tabela de POS)</param>
    /// <returns></returns>
    //Alterado para SqlServer
    public static bool incluirTerminaisAbertura(DateTime dataHora, bool abrir, string serial_number)
    {
        string abr;
        SqlParamsList ps = new SqlParamsList();
        if (abrir.Equals(true))
        {
            ps.Add(new Fields("@data_abertura", dataHora));
        }
        else
        {
            ps.Add(new Fields("@data_fechamento", dataHora));
            ps.Add(new Fields("@data_abertura", getDataAbertura(serial_number)));
        }

        abr = abrir == true ? "S" : "N";
      
        BD BD = new BD();
        if (abrir.Equals(true))
        {
            BD.ExecuteScalar("insert into terminais_abertura (data_abertura,status, pos_serial_number) values('" + dataHora.ToString("dd/MM/yyyy HH:mm:ss") + "','" + abr + "','" + serial_number + "')", null);
        }
        else
        {
            BD.ExecuteScalar("update terminais_abertura set data_fechamento = '" + dataHora.ToString("dd/MM/yyyy HH:mm:ss") + "', status = 'N' where pos_serial_number = '" + serial_number + "' and data_abertura = '" + getDataAbertura(serial_number).ToString("dd/MM/yyyy HH:mm:ss") + "'", null);
        }
        int qtd = 0;
        if (abrir.Equals(true))
        {
            qtd = Convert.ToInt32(BD.ExecuteScalar("select count(*) from terminais_abertura where data_abertura = '" + dataHora.ToString("dd/MM/yyyy HH:mm:ss") + "' and status = '" + abr + "' and pos_serial_number = '" + serial_number + "'", null));
        }
        else
        {
            qtd = Convert.ToInt32(BD.ExecuteScalar("select count(*) from terminais_abertura where data_fechamento = '" + dataHora.ToString("dd/MM/yyyy HH:mm:ss") + "' and status = '" + abr + "' and pos_serial_number = '" + serial_number + "'", null));
        }
        return qtd > 0;
    }

    //Alterado para SqlServer
    public static DateTime getDataAbertura(string numSerie)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@serial_number", numSerie));
        BD BD = new BD();
        DateTime dataAbertura = Convert.ToDateTime(BD.ExecuteScalar("select data_abertura from terminais_abertura where pos_serial_number = @serial_number and status = 'S' and data_abertura = "
                + " (select max(data_abertura) from terminais_abertura where pos_serial_number = @serial_number)", ps));
        return dataAbertura;
    }

    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="serial_number"></param>
    /// <param name="cred_id"></param>
    /// <returns></returns>
    public static string getUltimoCupom(string numSerie)
    {
      SqlParamsList ps = new SqlParamsList();
      ps.Add(new Fields("@serial_number", numSerie));
      BD BD = new BD();
      string cupom = Convert.ToString(BD.ExecuteScalar("select top 1 cupom from webpos_ultimos_cupons where serial_number = @serial_number",ps));
      return cupom;
    }

    public static bool insereCupomUltimosCupons(string cupom, string serialNumber)
    {
      SqlParamsList ps = new SqlParamsList();      
      ps.Add(new Fields("@serialNumber", serialNumber));
      BD BD = new BD();
      //deletando possível cupom existente
      BD.ExecuteScalar("delete from webpos_ultimos_cupons where serial_number = @serialNumber", ps);

      ps.Add(new Fields("@cupom", cupom));
      //inserindo o novo cupom
      BD.ExecuteScalar("insert into webpos_ultimos_cupons values(@cupom,@serialNumber)", ps);
      return Convert.ToInt32(BD.ExecuteScalar("select count(*) from webpos_ultimos_cupons where cupom = @cupom and serial_number = @serialNumber", ps)) > 0;
    }

    /// <summary>
    /// Retorna o usuario cadastrado para realizar a recarga conforme o numero do pos
    /// </summary>
    /// <param name="numSerie"></param>
    /// <returns></returns>
    //Alterado para SqlServer 
    public static string UsuarioRecarga(string numSerie)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@serial_number", numSerie));
        BD BD = new BD();
        return BD.ExecuteScalar("select usuario_recarga from pos where pos_serial_number = @serial_number", ps).ToString();
    }

      /// <summary>
      /// Retorna se estabelecimento utiliza recarga a vista, convenio ou os dois
      /// </summary>
      /// <param name="numSerie"></param>
      /// <returns>V - a vista, C - Convenio, T - os dois</returns>
    //Alterado para SqlServer
    public static string TipoRecarga(string numSerie)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@serial_number", numSerie));
        BD BD = new BD();
        return  BD.ExecuteScalar("select tipo_recarga from pos where pos_serial_number = @serial_number", ps).ToString();

    }

    /// <summary>
    /// retorna as informacoes da ultima abertura de teminal
    /// </summary>
    /// <param name="numSerie"></param>
    /// <returns></returns>
    //Alterado para SqlServer
    public static DataTable getFPeriodo(string numSerie)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@serial_number", numSerie));
        BD BD = new BD();
        return BD.GetDataTable("select ta.data_abertura, ta.data_fechamento, p.cred_id from terminais_abertura ta, pos p where ta.pos_serial_number = @serial_number and "
            + " ta.pos_serial_number = p.pos_serial_number and ta.data_abertura = (select max(data_abertura) from terminais_abertura where pos_serial_number = @serial_number)", ps);
    }

    /// <summary>
    /// Obtem as autorizacoes do estabelecimento
    /// </summary>
    /// <param name="numSerie"></param>
    /// <returns></returns>
    //Alterado para SqlServer
    public static DataTable getContaCorrente(string numSerie)
    {
        string sql = "";
        DataTable dt = new DataTable();
        BD BD = new BD();

        dt = getFPeriodo(numSerie);

        if (dt.Rows.Count > 0)
        {
            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("@dataini", dt.Rows[0]["data_abertura"]));
            ps.Add(new Fields("@datafin", dt.Rows[0]["data_fechamento"]));
            ps.Add(new Fields("@cred_id", dt.Rows[0]["cred_id"]));

            sql = "select sum(cc.debito) as valor, cc.cred_id, c.codcartimp, cc.formapagto_id, f.descricao, ";
            sql += " cast(concat(day(cc.datavenda),'.',month(cc.datavenda),'.',";
            sql += " year(cc.datavenda),' 00:00:00') as smalldatetime) as data ";
            sql += " from contacorrente cc ";
            sql += " inner join cartoes c on cc.cartao_id = c.cartao_id ";
            sql += " inner join formaspagto f on cc.formapagto_id = f.forma_id ";
            sql += " where datavenda between @dataini and @datafin and cc.cred_id = @cred_id and cc.cancelada = 'N' and cc.operador = 'POS.DB' ";
            sql += " group by cc.cred_id, c.codcartimp, cc.formapagto_id, f.descricao, cc.trans_id, data, cc.datavenda ";
            sql += " order by data, cc.formapagto_id ";

            dt = BD.GetDataTable(sql, ps);
        }
        return dt;
    }

    //Alterado para SqlServer
    public static DateTime getDataVenda(string trans_id)
    {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@trans_id", trans_id));
        BD BD = new BD();
        return Convert.ToDateTime(BD.ExecuteScalar("select top 1 datavenda from contacorrente where trans_id = @trans_id", ps));

    }
  }
}
