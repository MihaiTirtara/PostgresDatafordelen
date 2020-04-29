using System;
using System.IO;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Newtonsoft.Json.Linq;
using System.Linq;



namespace PostgresUpdate
{
    class Program
    {

        private static string con_string = "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=GEO_DATA";
        private static NpgsqlConnection conn = new NpgsqlConnection(con_string);
        private static List<String> idList = new List<string>();
        private static Dictionary<string, string> lookUpTable;
        private  static List<String> hashList = new List<string>();

        static void Main(string[] args)
        {
            conn.Open();
            conn.TypeMapper.UseJsonNet();
            Console.WriteLine("Connection done");
            //ProcessDirectory("/home/mehigh/GeoData","GEO-topic");
            //createLists(conn);
            ProcessDirectory("/home/mehigh/GeoData");
            //postgresTrial();
        }

        public static void ProcessDirectory(string targetDirectory)
        {
            string[] fileEntries = Directory.GetFiles(targetDirectory);
            foreach (string filenName in fileEntries)
            {
                Console.WriteLine(filenName);
                PostgresInsert(filenName);
                //updateDb(filenName);
            }
        }

        public static void PostgresInsert(String filename)
        {
            String jsonDoc = "";
            List<String> batch = new List<string>();
            int i = 0;

            using (var streamReader = new StreamReader(filename))
            {
                using (var reader = new Newtonsoft.Json.JsonTextReader(streamReader))
                {
                    while (reader.Read())
                    {
                        if (reader.TokenType == Newtonsoft.Json.JsonToken.StartArray)
                        {
                            //Console.WriteLine(reader.TokenType);
                            while (reader.Read())
                            {
                                if (reader.TokenType == Newtonsoft.Json.JsonToken.StartObject)
                                {
                                    Object obj = new Newtonsoft.Json.JsonSerializer().Deserialize(reader);
                                    jsonDoc = Newtonsoft.Json.JsonConvert.SerializeObject(obj);
                                }

                                batch.Add(jsonDoc);
                                //Console.WriteLine(batch.Count);
                                if (batch.Count >= 10000)
                                {
                                    i += 10000;
                                    MD5 md5Hash = MD5.Create();

                                    // Insert some data
                                    using (var writer = conn.BeginBinaryImport("COPY geo (data,id,hash) FROM STDIN (FORMAT BINARY)"))
                                    {
                                        foreach (var document in batch)
                                        {
                                            writer.StartRow();
                                            writer.Write(document, NpgsqlDbType.Json);

                                            var jo = JObject.Parse(document);
                                            var id = jo["properties"]["gml_id"].ToString();
                                            writer.Write(id);

                                            var hash = GetMd5Hash(md5Hash, document);
                                            writer.Write(hash);
                                        }

                                        writer.Complete();
                                        batch.Clear();
                                        Console.WriteLine(i);
                                    }
                                }


                            }
                        }
                    }
                }
            }

        }

        public static string GetMd5Hash(MD5 md5hash, string input)
        {
            byte[] data = md5hash.ComputeHash(Encoding.UTF8.GetBytes(input));

            StringBuilder sBuilder = new StringBuilder();

            for (int i = 0; i < data.Length; i++)
            {
                sBuilder.Append(data[i].ToString("x2"));
            }

            return sBuilder.ToString();
        }

        public static void createLists(NpgsqlConnection conn)
        {
       

            using (var cmd = new NpgsqlCommand("SELECT id  from geo", conn))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    Console.WriteLine(reader.GetFieldValue<String>(0));
                    idList.Add(reader.GetFieldValue<String>(0));
                }
            }


            using (var cmd = new NpgsqlCommand("SELECT hash from geo", conn))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    Console.WriteLine(reader.GetFieldValue<String>(0));
                    hashList.Add(reader.GetFieldValue<String>(0));
                }
                Console.WriteLine(hashList.Count);
                Console.WriteLine(idList.Count);
                //ProcessDirectory("/home/mehigh/GeoData2");


            }

           


        }

        public static void updateDb(String filename)
        {
            Console.WriteLine(idList.Count);
            Console.WriteLine(hashList.Count);

            
            string jsonDoc = "";
            List<String> batch = new List<string>();
            int i = 0;

            lookUpTable = idList.ToDictionary(x => x, x => x);
            var hashListDictionary = hashList.ToDictionary(x => x, x => x);

            using (var streamReader = new StreamReader(filename))
            {
                using (var reader = new Newtonsoft.Json.JsonTextReader(streamReader))
                {
                    while (reader.Read())
                    {
                        if (reader.TokenType == Newtonsoft.Json.JsonToken.StartArray)
                        {
                            //Console.WriteLine(reader.TokenType);
                            while (reader.Read())
                            {
                                if (reader.TokenType == Newtonsoft.Json.JsonToken.StartObject)
                                {
                                    Object obj = new Newtonsoft.Json.JsonSerializer().Deserialize(reader);
                                    jsonDoc = Newtonsoft.Json.JsonConvert.SerializeObject(obj);
                                }

                                batch.Add(jsonDoc);
                                //Console.WriteLine(batch.Count);
                                if (batch.Count >= 10000)
                                {
                                    i += 10000;
                                    MD5 md5Hash = MD5.Create();

                                    // Insert some data

                                    foreach (var document in batch)
                                    {
                                        var jo = JObject.Parse(document);
                                        var id = jo["properties"]["gml_id"].ToString();
                                        var hash = GetMd5Hash(md5Hash, document);
                                        //Compare data
                                        //bool matchingId = idList.AsParallel().Any(s => s.Contains(id));
                                        
                                        bool matchingId = lookUpTable.ContainsKey(id);

                                        if (matchingId)
                                        {
                                            // bool matchingHash = hashList.AsParallel().Any(s => s.Contains(hash));
                                            var matchingHash = hashListDictionary.ContainsKey(hash);
                                            if (!matchingHash)
                                            {
                                                using (var cmd = new NpgsqlCommand("UPDATE geo SET data = :userdata, hash = :userhash WHERE id = :userid ", conn))
                                                {
                                                    cmd.Parameters.Add(new NpgsqlParameter("userdata", NpgsqlDbType.Json));
                                                    cmd.Parameters.Add(new NpgsqlParameter("userid", NpgsqlDbType.Text));
                                                    cmd.Parameters.Add(new NpgsqlParameter("userhash", NpgsqlDbType.Text));

                                                    cmd.Parameters[0].Value = document;
                                                    cmd.Parameters[1].Value = id;
                                                    cmd.Parameters[2].Value = hash;

                                                    cmd.Prepare();
                                                    cmd.ExecuteNonQuery();

                                                    Console.WriteLine("Object updated " + id);

                                                }

                                            }
                                        }
                                        else
                                        {
                                            using (var cmd = new NpgsqlCommand("INSERT INTO geo(data,id,hash) VALUES (:userdata,:userid,:userhash) ", conn))
                                            {
                                                cmd.Parameters.Add(new NpgsqlParameter("userdata", NpgsqlDbType.Json));
                                                cmd.Parameters.Add(new NpgsqlParameter("userid", NpgsqlDbType.Text));
                                                cmd.Parameters.Add(new NpgsqlParameter("userhash", NpgsqlDbType.Text));

                                                cmd.Parameters[0].Value = document;
                                                cmd.Parameters[1].Value = id;
                                                cmd.Parameters[2].Value = hash;

                                                cmd.Prepare();
                                                cmd.ExecuteNonQuery();

                                                Console.WriteLine("Object inserted " + id);

                                            }
                                        }

                                    }
                                    batch.Clear();
                                    Console.WriteLine(i);

                                }


                            }
                        }
                    }
                }
            }
            
        }

}

}
