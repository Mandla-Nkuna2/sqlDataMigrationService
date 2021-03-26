using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;
using dataMigrationService.services;
namespace dataMigrationService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DataController : ControllerBase
    {
        DataExtractor data = new DataExtractor();
        public string GetAll()
        {
            return "Add connection string to body";
        }



        [HttpGet("startMigration")]
        public string Test([FromQuery] string connString, string companyName)
        {
            data.startDataMigration(connString, companyName);
            return "starting" + companyName;
        }

    }
}
