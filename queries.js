// Connects to plp_bookstore and runs the required queries, aggregations, indexes and explain plans.

const { MongoClient } = require('mongodb');
const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collName = 'books';

async function main() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const coll = db.collection(collName);

    console.log('1) Find all books in genre "Fiction" (projection):');
    console.log(await coll.find({ genre: 'Fiction' }, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray());

    console.log('\n2) Find books published after 2000 (projection):');
    console.log(await coll.find({ published_year: { $gt: 2000 } }, { projection: { title: 1, published_year: 1, _id: 0 } }).toArray());

    console.log('\n3) Find books by author "George Orwell":');
    console.log(await coll.find({ author: 'George Orwell' }).toArray());

    console.log('\n4) Update price of "1984" to 12.50:');
    await coll.updateOne({ title: '1984' }, { $set: { price: 12.5 } });
    console.log('Updated:', await coll.findOne({ title: '1984' }));

    console.log('\n5) Delete book by title "Moby Dick":');
    await coll.deleteOne({ title: 'Moby Dick' });
    console.log('Exists now?', await coll.findOne({ title: 'Moby Dick' }));

    console.log('\n6) Find books in stock and published after 2010 (projection):');
    console.log(await coll.find({ in_stock: true, published_year: { $gt: 2010 } }, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray());

    console.log('\n7) Sort by price ascending:');
    console.log(await coll.find({}, { projection: { title: 1, price: 1, _id: 0 } }).sort({ price: 1 }).toArray());

    console.log('\n8) Sort by price descending:');
    console.log(await coll.find({}, { projection: { title: 1, price: 1, _id: 0 } }).sort({ price: -1 }).toArray());

    console.log('\n9) Pagination (page 1, 5 per page):');
    console.log(await coll.find({}, { projection: { title: 1, _id: 0 } }).skip(0).limit(5).toArray());

    console.log('\n10) Aggregation: average price by genre:');
    console.log(await coll.aggregate([
      { $group: { _id: '$genre', avgPrice: { $avg: '$price' }, count: { $sum: 1 } } },
      { $sort: { avgPrice: -1 } }
    ]).toArray());

    console.log('\n11) Aggregation: author with most books:');
    console.log(await coll.aggregate([
      { $group: { _id: '$author', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray());

    console.log('\n12) Aggregation: group by decade and count:');
    console.log(await coll.aggregate([
      { $group: { _id: { $multiply: [ { $floor: { $divide: ['$published_year', 10] } }, 10 ] }, count: { $sum: 1 } } },
      { $sort: { _id: 1 } }
    ]).toArray());

    console.log('\n13) Create indexes (title, compound author+published_year):');
    console.log('index title:', await coll.createIndex({ title: 1 }));
    console.log('index author+published_year:', await coll.createIndex({ author: 1, published_year: 1 }));

    console.log('\n14) Explain plan for a title search (executionStats):');
    console.log(await coll.find({ title: 'The Hobbit' }).explain('executionStats'));

  } catch (err) {
    console.error(err);
  } finally {
    await client.close();
    console.log('Done.');
  }
}

main().catch(console.error);