// Populate the city table with the data from the cityData array
// This script will add the cities to the city table if they do not already exist
// Run this script using the following command: ts-node apps/backend/src/seeding/city.seed.ts

import { MikroORM } from '@mikro-orm/core';
import { City } from '../user/entities/info.entity';
import { SqlHighlighter } from '@mikro-orm/sql-highlighter';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';
import 'dotenv/config';

const cityData = [
  'Mumbai',
  'Delhi',
  'Bangalore',
  'Kolkata',
  'Chennai',
  'Hyderabad',
  'Pune',
  'Ahmedabad',
  'Surat',
  'Jaipur',
  'Lucknow',
  'Kanpur',
  'Nagpur',
  'Visakhapatnam',
  'Indore',
  'Thane',
  'Bhopal',
  'Patna',
  'Vadodara',
  'Ghaziabad',
  'Ludhiana',
  'Agra',
  'Nashik',
  'Ranchi',
  'Faridabad',
  'Meerut',
  'Rajkot',
  'Varanasi',
  'Srinagar',
  'Amritsar',
  'Allahabad',
  'Howrah',
  'Jabalpur',
  'Coimbatore',
  'Madurai',
  'Aurangabad',
  'Dhanbad',
  'Gwalior',
  'Vijayawada',
  'Jodhpur',
  'Raipur',
  'Kochi',
  'Jabalpur',
  'Guwahati',
  'Dhanbad',
  'Amritsar',
  'Allahabad',
  'Hubli-Dharwad',
  'Solapur',
  'Tiruchirappalli',
  'Bareilly',
  'Moradabad',
  'Mysore',
  'Tiruppur',
  'Gurgaon',
  'Aligarh',
  'Jalandhar',
  'Bhubaneswar',
  'Salem',
  'Warangal',
  'Guntur',
  'Saharanpur',
  'Gorakhpur',
  'Bhiwandi',
  'Jammu',
  'Jalgaon',
  'Kota',
  'Bikaner',
  'Tirunelveli',
  'Ujjain',
  'Sangli',
  'Tumkur',
  'Hisar',
  'Rohtak',
  'Tirupur',
  'Udaipur',
  'Noida',
  'Calicut',
  'Visakhapatnam',
  'Thiruvananthapuram',
  'Thrissur',
  'Pondicherry',
  'Mangalore',
  'Goa',
  'Anywhere in India',
  'Outside India',
];

async function main() {
  const orm = await MikroORM.init<PostgreSqlDriver>({
    entities: [City],
    clientUrl: process.env.DATABASE_URL as string,
    highlighter: new SqlHighlighter(),
    driver: PostgreSqlDriver,
    debug: true,
    driverOptions: {
      connection: { ssl: { rejectUnauthorized: false } },
    },
  });

  const em = orm.em.fork();

  for (const cityName of cityData) {
    // Check if the city already exists
    const existingCity = await em.findOne(City, { name: cityName });
    if (!existingCity) {
      // Create a new city instance
      const city = new City(cityName);
      // Persist the new city
      em.persist(city);
      console.log(`City ${cityName} added.`);
    } else {
      console.log(`City ${cityName} already exists.`);
    }
  }
  await em.flush();

  console.log('Cities have been populated successfully.');
  await orm.close(true);
}

main().catch(console.error);
