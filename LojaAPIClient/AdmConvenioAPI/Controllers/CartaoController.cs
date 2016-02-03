using AdmConvenioAPI.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;


namespace AdmConvenioAPI.Controllers
{
    public class CartaoController : ApiController
    {
        public String GET()
        {
            Cartao cartao = new Cartao();
            DataTable dt = DAL.DALCartao.BuscarTodosCartoesCantinex(174244);
            foreach (DataRow row in dt.Rows)
	        {

                cartao.Nome = Convert.ToString(row["nome"]);
                cartao.CodCartImp = Convert.ToString(row["codcartimp"]);
                
	        }


            var javaScriptSerializer = new System.Web.Script.Serialization.JavaScriptSerializer();
            string jsonString = javaScriptSerializer.Serialize(cartao);
            
            return jsonString;
        }
    }
}
