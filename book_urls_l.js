printjson(db.L_BOOKS_FINAL_PRIME.aggregate({ $group : { _id : '$book_url', count: {$sum : 1} } }, {$sort :{count:-1}}, { $limit : 4000 }))
