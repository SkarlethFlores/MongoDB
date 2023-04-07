const contents = [
    {
        content: "/Users/admin/Library/Application Support/NoSQLBooster for MongoDB/databases/movies.json",
        collection: "movies",
        idPolicy: "overwrite_with_same_id",
        //overwrite_with_same_id|always_insert_with_new_id|insert_with_new_id_if_id_exists|skip_documents_with_existing_id|abort_if_id_already_exists|drop_collection_first|log_errors
        //Use the transformer to customize the import result
        //transformer: (doc)=>{ //async (doc)=>{
        //   doc["importDate"]= new Date();
        //   doc["oid"] = mb.convert({input:doc["oid"], to:"objectId", onError:"remain_unchanged", onNull:null});  //to: double|string|objectId|bool|date|int|long|decimal
        //   return doc; //return null skips this doc
        //}
    }
];

mb.importContent({
    connection: "localhost",
    database: "clases",
    fromType: "file",
    batchSize: 2000,
    contents
})


// 1. Analizar con find la colección.  
db.movies.find()


// 2. Contar cuántos documentos (películas) tiene cargado.
var query = { "year": { $gt: 0 } }
db.movies.find(query).count()


// 3. Insertar una película.
var pelicula_nueva = {
    "title": "Everithing everywhere all at once",
    "year": 2022,
    "cast": ["Michelle Yeon", "Jamie Lee Curtis", "Stephanie Hsu"],
    "genres": ["comedy", "supernatural"]
}
db.movies.insertOne(pelicula_nueva)


// 4. Borrar la película insertada en el punto anterior (en el 3).
var pelicula_nueva = { "title": "Everithing everywhere all at once" }
db.movies.deleteOne(pelicula_nueva)


//5. Contar cuantas películas tienen actores (cast) que se llaman “and”. Estos nombres de actores están por ERROR.  
var queryActor = { "cast": { $in: ['and'] } }
db.movies.find(queryActor).count()


// 6. Actualizar los documentos cuyo actor (cast) tenga por error el valor “and” como si realmente  
//fuera un actor. Para ello, se debe sacar únicamente ese valor del array cast. Por lo tanto, no se 
//debe eliminar ni el documento (película) ni su array cast con el resto de actores.  

var queryActor = { "cast": { $in: ['and'] } }
var operation = { $pull: { "cast": "and" } }
db.movies.updateMany(queryActor, operation)


// 7. Contar cuantos documentos (películas) tienen el array ‘cast’ vacío.  
var queryActor = { "cast": { $size: 0 } }
db.movies.find(queryActor).count()


//8. Actualizar TODOS los documentos (películas) que tengan el array cast vacío, añadiendo un nuevo elemento dentro 
//del array con valor Undefined. Cuidado! El tipo de cast debe seguir siendo un array. El array debe ser así -> [
// "Undefined" ].

var queryCast = { "cast": { $size: 0 } }
var operacion = { $push: { "cast": "Undefined" } }
db.movies.updateMany(queryCast, operacion)

//Comprobar la estructura es aun tipo array

var queryCastU = { "cast": "Undefined" }
db.movies.find(queryCastU)

// 9. Contar cuantos documentos (películas) tienen el array genres vacío.
var queryGenres = { "genres": { $size: 0 } }
db.movies.find(queryGenres).count()


// 10. Actualizar TODOS los documentos (películas) que tengan el array genres vacío, añadiendo 
// un nuevo elemento dentro del array con valor Undefined. Cuidado! 
// El tipo de genres debe seguir siendo un array. El array debe ser así -> ["Undefined" ].

var queryGenres = { "genres": { $size: 0 } }
var operacion = { $push: { "genres": "Undefined" } }
db.movies.updateMany(queryGenres, operacion)

//Comprobar la estructura es aun tipo array

var queryGenresU = { "genres": "Undefined" }
db.movies.find(queryGenresU)


// 11. Mostrar el año más reciente / actual que tenemos sobre todas las películas.  

db.movies.find().sort({ year: -1 }).limit(1)


//12. Contar cuántas películas han salido en los últimos 20 años. Debe hacerse desde el último año 
// que se tienen registradas películas en la colección, mostrando el resultado total de esos años. 
// Se debe hacer con el Framework de Agregación. 

var fase1 = { $group: { "_id": "$year", "total": { $sum: 1 } } }
var fase2 = { $project: { "_id": 0, "year": "$_id" "total": 1 } }
var fase3 = { $sort: { "year": -1 } }
var fase4 = { $limit: 20 }
var fase5 = { $group: { "_id": null, "total": { $sum: "$total" } } }
db.movies.aggregate([fase1, fase2, fase3, fase4, fase5])


// 13. Contar cuántas películas han salido en la década de los 60 (del 60 al 69 incluidos). 
// Se debe hacer con el Framework de Agregación.

var fase1 = { $group: { "_id": "$year", "total": { $sum: 1 } } }
var fase2 = { $match: { "_id": { $gte: 1960 } } }
var fase3 = { $match: { "_id": { $lte: 1969 } } }
var fase4 = { $group: { "_id": null, "total": { $sum: "$total" } } }
db.movies.aggregate([fase1, fase2, fase3, fase4])


// 14. Mostrar el año u años con más películas mostrando el número de películas de ese año. 
// Revisar si varios años pueden compartir tener el mayor número de películas. 

var YearArray = { "year": "$year" }
//Agrupar por año
var fase1 = { $group: { "_id": "$year", "numero_peliculas": { $sum: 1 } } }
//renombrar id por año
var fase2 = { $project: { "_id": 0, "year": "$_id", "numero_peliculas": 1 } }
var fase3 = { $group: { _id: "$numero_peliculas", "number_years": { $sum: 1 }, "items": { $push: YearArray } } }
//Agrupar por numero de peliculas, para encontrar todos los años que tienen en mismo numero de peliculas. 
//renombrar _id por numero de peliculas
var fase4 = { $project: { "_id": 0, "numero_peliculas": "$_id", "years": "$items" } }
//Ordenar descendentemente y limitar a 1 para mostrar solo el maximo numero de peliculas,
//con los años que correspondan a ese mismo # de peliculas.
var fase5 = { $sort: { "numero_peliculas": -1 } }
var fase6 = { $limit: 1 }
var fase7 = { $project: { "_id_year": "$years.year", "# pelis": "$numero_peliculas" } }
db.movies.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7])



// 15. Mostrar el año u años con menos películas mostrando el número de películas de ese año. 
// Revisar si varios años pueden compartir tener el menor número de películas. 

var YearArray = { "year": "$year" }
var fase1 = { $group: { "_id": "$year", "numero_peliculas": { $sum: 1 } } }
var fase2 = { $project: { "_id": 0, "year": "$_id", "numero_peliculas": 1 } }
var fase3 = { $group: { _id: "$numero_peliculas", "number_years": { $sum: 1 }, "items": { $push: YearArray } } }
var fase4 = { $project: { "_id": 0, "numero_peliculas": "$_id", "years": "$items" } }
var fase5 = { $sort: { "numero_peliculas": 1 } }
var fase6 = { $limit: 1 }
var fase7 = { $project: { "_id_years": "$years.year"  "# pelis": "$numero_peliculas" } }
db.movies.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7])


// 16. Guardar en nueva colección llamada “actors” realizando la fase $unwind por actor. Después, 
//contar cuantos documentos existen en la nueva colección.

var fase1 = { $unwind: "$cast" }
var fase2 = { $project: { "_id": 0 } } // Para evitar claves duplicadas en la db
var fase3 = { $out: "actors" }
db.movies.aggregate([fase1, fase2, fase3])
//db.actors.find()
db.actors.find().count()


//17. Sobre actors (nueva colección), mostrar la lista con los 5 actores que han participado en más 
// películas mostrando el número de películas en las que ha participado. Importante! Se necesita previamente filtrar 
// para descartar aquellos actores llamados "Undefined". 
//Aclarar que no se eliminan de la colección, sólo que filtramos para que no aparezcan.

var fase1 = { $match: { "cast": { $ne: 'Undefined' } } }
var fase2 = { $group: { "_id": "$cast", "numero_peliculas": { $sum: 1 } } }
var fase3 = { $sort: { "numero_peliculas": -1 } }
var fase4 = { $limit: 5 }
db.actors.aggregate([fase1, fase2, fase3, fase4])


// 18. Sobre actors (nueva colección), agrupar por película y año mostrando las 5 en las que más actores hayan 
// participado, mostrando el número total de actores.

var fase1 = { $match: { "cast": { $ne: 'Undefined' } } }
var fase2 = { $group: { "_id": { "title": "$title", "year": "$year" }, "numero_actors": { $sum: 1 } } }
var fase3 = { $sort: { "numero_actors": -1 } }
var fase4 = { $limit: 5 }
db.actors.aggregate([fase1, fase2, fase3, fase4])



//19. Sobre actors (nueva colección), mostrar los 5 actores cuya carrera haya sido la más larga. Para ello, se debe
//mostrar cuándo comenzó su carrera, cuándo finalizó y cuántos años ha trabajado. Importante! Se necesita previamente
//filtrar para descartar aquellos actores llamados "Undefined". Aclarar que no se eliminan de la colección, sólo que
//filtramos para que no aparezcan.

var fase1 = { $match: { "cast": { $ne: 'Undefined' } } }
var ArrayYears = { "year": "$year" }
var fase2 = { $group: { _id: "$cast", "years": { $push: ArrayYears } } }
var fase3 = { $project: { "_id": 1, "years": "$years.year" } }
var fase4 = { $addFields: { "comienza": { $min: "$years" }, "termina": { $max: "$years" } } }
var fase5 = { $addFields: { "anos": { $subtract: ["$termina", "$comienza"] } } }
var fase6 = { $project: { "_id": 1, "comienza": 1, "termina": 1, "anos": 1 } }
var fase7 = { $sort: { "anos": -1 } }
var fase8 = { $limit: 5 }
db.actors.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7, fase8])


// 20. Sobre actors (nueva colección), Guardar en nueva colección llamada “genres” realizando la fase 
//$unwind por genres. Después, contar cuantos documentos existen en la nueva colección.

var fase1 = { $unwind: "$genres" }
var fase2 = { $project: { "_id": 0 } } // Para evitar claves duplicadas en la db
var fase3 = { $out: "genres" }
db.movies.aggregate([fase1, fase2, fase3])
db.genres.find().count()


//21. Sobre genres (nueva colección), mostrar los 5 documentos agrupados por “Año y Género” que más número de
// películas diferentes tienen mostrando el número total de películas.  

var ArrayTitles = { "title": "$title" }
var fase1 = { $group: { "_id": { "year": "$year", "genre": "$genres" }, "pelis": { $push: ArrayTitles } } }
var fase2 = { $unwind: "$pelis" }

var fase3 = { $project: { "_id": 0, "ano_genero": "$_id", "pelis": "$pelis.title" } }
var fase4 = { $group: { "_id": { "ano_genero": "$ano_genero", "pelis": "$pelis" }, "count": { $sum: 1 } } }

var fase5 = { $project: { "_id": { "year": "$_id.ano_genero.year", "genre": "$_id.ano_genero.genre" }, "pelis": "$_id.pelis" } }
var fase6 = { $group: { "_id": "$_id", "count": { $sum: 1 } } }

var fase7 = { $sort: { "count": -1 } }
var fase8 = { $limit: 5 }
db.genres.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7, fase8])


// 22. Sobre genres (nueva colección), mostrar los 5 actores y los géneros en los que han participado con más número de
//géneros diferentes, se debe mostrar el número de géneros diferentes que ha interpretado. Importante! Se necesita
//previamente filtrar para descartar aquellos actores llamados "Undefined". Aclarar que no se eliminan de la colección,
//sólo que filtramos para que no aparezcan.

var fase1 = { $match: { "cast": { $ne: 'Undefined' } } }
var fase2 = { $unwind: "$cast" }
var fase3 = { $group: { "_id": { "cast": "$cast", "genres": "$genres" }, "count": { $sum: 1 } } }
var fase4 = { $project: { "_id": "$_id.cast", "generos": "$_id.genres" } }

var ArrayGenres = { "generos": "$generos" }
var fase5 = { $group: { "_id": "$_id", "numgeneros": { $sum: 1 }, "generos": { $push: ArrayGenres } } }
var fase6 = { $project: { "_id": 1, "numgeneros": 1, "generos": "$generos.generos" } }
var fase7 = { $sort: { "numgeneros": -1 } }
var fase8 = { $limit: 5 }
db.genres.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7, fase8])


//23. Sobre genres (nueva colección), mostrar las 5 películas y su año correspondiente en los que más géneros diferentes
//han sido catalogados, mostrando esos géneros y el número de géneros que contiene.

var ArrayGenres = { "generos": "$genres" }
var fase2 = { $group: { "_id": { "title": "$title", "year": "$year" }, "generos": { $push: ArrayGenres } } }
var fase3 = { $project: { "_id": 1, "generos": "$generos.generos" } }
var fase4 = { $unwind: "$generos" }
var fase5 = { $group: { "_id": { "_id": "$_id", "generos": "$generos" }, "count": { $sum: 1 } } }
var fase6 = { $project: { "_id": "$_id._id", "generos": "$_id.generos" } }

var ArrayGenres = { "generos": "$generos" }
var fase7 = { $group: { "_id": "$_id", "numgeneros": { $sum: 1 }, "generos": { $push: ArrayGenres } } }
var fase8 = { $sort: { "numgeneros": -1 } }
var fase9 = { $limit: 5 }
var fase10 = { $project: { generos: "$generos.generos", "numgeneros": "$numgeneros" } }
db.genres.aggregate([fase2, fase3, fase4, fase5, fase6, fase7, fase8, fase9, fase10])



// 24. Query libre sobre el pipeline de agregación.
//Veamos en cuantas peliculas en los ultimos 23 años, en las que ha aparecido un actor llamado Brad, 
//presentando el total de peliculas para cada actor cuyo nombre incluye 'Brad. Ordenar descendentemente por numero de peliculas.
var fase1 = { $match: { "year": { $gte: 2000 } } }
var fase2 = { $match: { 'cast': { '$regex': /Brad/ } } }
var fase3 = { $group: { "_id": { "cast": "$cast" }, "Num_pelis": { $sum: 1 } } }
var fase5 = { $sort: { "Num_pelis": -1 } }
db.actors.aggregate(fase1, fase2, fase3, fase5)


// 25. Query libre sobre el pipeline de agregación.
//crear una nueva colleccion con todas las peliculas en las que ha aparecido Brad Pitt, y que tienen un genero definido.
//Mostrar la nueva coleccion ordenada en forma descendente por año.
var fase1 = { $set: { "size_of_gen": { $size: "$genres" } } }
var fase2 = { $match: { "size_of_gen": { $gt: 0 } } }
var fase3 = { $project: { "_id": 1, "cast": "$cast", "year": "$year", "title": "$title", "genres": "$genres" } }
var fase4 = { $unwind: "$cast" }
var fase5 = { $project: { "_id": 0 } }
var fase6 = { $match: { 'cast': { '$regex': /Brad Pitt/ } } }
var fase7 = { $out: "BradPitt" }
db.movies.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7])

var fase8 = { $project: { "_id": "$_id", "cast": "$cast", "genres": "$genres", "year": "$year", "title": "$title" } }
var fase9 = { $sort: { "year": -1 } }
db.BradPitt.aggregate(fase8, fase9)



// 26. Query libre sobre el pipeline de agregación 
//Usando la nueva coleccion (BradPitt), Mostrar cuantas peliculas ha hecho por año, que incluyan el genero "Drama", ordenando descendentemente por numero de peliculas.

var fase1 = { $match: { "genres": { $eq: "Drama" } } }
var fase2 = { $group: { "_id": { "year": "$year" }, "N_pelis": { $sum: 1 } } }
var fase3 = { $project: { "_id": "$_id", "cast": "BradPitt", "genre": "Drama", "N_pelis": "$N_pelis" } }
var fase4 = { $sort: { "N_pelis": -1 } }
db.BradPitt.aggregate([fase1, fase2, fase3, fase4])







