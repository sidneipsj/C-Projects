using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Web;

namespace Financas.Entidades
{
    public class Usuario
    {
        public int Id { get; set; }
        [Required]
        public String Nome { get; set; }
        [Required, EmailAddress]
        public String Email { get; set; }
    }
}