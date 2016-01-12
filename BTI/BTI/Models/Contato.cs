using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace BTI.Models
{
    public class Contato
    {
        public int Id { get; set; }
        public String Nome { get; set; }
        public String Ramal { get; set; }
        public String Email { get; set; }
        public Setor Setor { get; set; }

    }
}