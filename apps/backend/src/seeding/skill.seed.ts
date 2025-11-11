// Populate the skills table with the data from the skillData array
// Clear all the data from the skills table before running this script
// Or change the start index or use findone method to avoid duplicate entries
// Run this script using the following command: ts-node apps/backend/src/seeding/skill.seed.ts
import { MikroORM } from '@mikro-orm/core';
import { Skill } from '../user/entities/info.entity';
import { SqlHighlighter } from '@mikro-orm/sql-highlighter';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';
import 'dotenv/config';

const skillData = [
  'Communication',
  'Negotiation',
  'Persuasion',
  'Time management',
  'Presentation skills',
  'Team work',
  'Active Listening',
  'Client engagement',
  'Relationship building',
  'Collaboration',
  'Networking',
  'Resilience',
  'Social selling',
  'Excel',
  'PowerPoint',
  'Word',
  'Account management',
  'Automation',
  'Cascading style sheets (CSS)',
  'Sales Engineering',
  'Sales operations',
  'CRM Software',
  'Salesforce',
  'HubSpot',
];

async function main() {
  const orm = await MikroORM.init<PostgreSqlDriver>({
    entities: [Skill],
    clientUrl: process.env.DATABASE_URL as string,
    highlighter: new SqlHighlighter(),
    driver: PostgreSqlDriver,
    debug: true,
    driverOptions: {
      connection: { ssl: { rejectUnauthorized: false } },
    },
  });

  const em = orm.em.fork();
  let index = 1; // Starting index for skill IDs

  for (const skillName of skillData) {
    // Create a new skill instance with a unique ID
    const skill = new Skill(skillName);
    skill.id = index++;
    // Persist the new skill
    em.persist(skill);
    console.log(`Skill ${skillName} added with ID ${skill.id}.`);
  }

  await em.flush();

  console.log('Skills have been populated successfully.');
  await orm.close(true);
}

main().catch(console.error);
