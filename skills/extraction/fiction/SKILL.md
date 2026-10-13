---
name: Fiction Extractor
description: Extract characters, locations, relationships, and events from fiction and narrative text
---

# Fiction / Narrative Extraction Strategy

You are extracting structured knowledge from fiction or narrative text. Focus on building a durable knowledge graph of the story's world, not a play-by-play of the plot.

## Entity Types

Use these system types for fictional content:

- **Character** -> `person`: Any named character (Alice, the White Rabbit, the Queen of Hearts)
- **Location** -> `location`: Named places (Wonderland, the rabbit hole, the garden)
- **Object** -> `device`: Significant named objects (the looking-glass, the vorpal sword)
- **Creature/Species** -> `other`: Types of beings (Cheshire Cat as species/type, Jabberwock)
- **Social Group** -> `group`: Named groups, factions, courts (the Court of Hearts, the jury)
- **Event** -> `topic`: Named or significant events (the trial, the tea party, the caucus race)

## Key Predicates

Prefer these predicates. Use snake_case, present tense:

**Relationships**: `knows`, `related_to`, `sibling_of`, `parent_of`, `child_of`, `friend_of`, `enemy_of`, `serves`, `rules_over`, `reports_to`

**Attributes**: `has_title`, `has_role`, `has_trait`, `has_ability`, `has_species`, `has_occupation`

**Spatial**: `lives_in`, `located_in`, `visited`, `originated_from`

**Possession**: `possesses`, `created`, `controls`

**Group**: `member_of`, `leads`, `belongs_to`

**Events**: `participated_in`, `witnessed`, `caused`

## What to Extract

- **Permanent character relationships**: who knows whom, family bonds, alliances, rivalries
- **Character attributes**: titles, roles, species, notable traits or abilities
- **Location relationships**: where characters live, where places are relative to each other
- **Group memberships**: who belongs to what faction, court, or social group
- **Object ownership**: who possesses significant items
- **Recurring interactions** that establish durable relationships (not one-off actions)
- **Character transformations**: changes in status, role, or nature

## What to Skip

- **Momentary actions**: "Alice picked up the cup", "he ran across the field" — these are plot mechanics, not knowledge
- **Dialogue content**: "she said 'how curious'" — unless it establishes a fact ("I am the Queen")
- **Narrative description**: "the room was dark and cold", "she felt frightened" — atmosphere, not facts
- **Emotional reactions**: "he was angry", "she wondered" — internal states, not relationships
- **Movement between scenes**: "they walked to the garden", "she fell down the hole" — plot progression
- **Repetitive re-statements**: if Alice knows the White Rabbit, don't extract it from every chapter

## Significance Criteria

- **High**: Character-to-character relationships, group memberships, character roles/titles, world-building facts that define the story's structure
- **Medium**: Character attributes, location details, one-time significant events, object ownership
- **Low**: Minor interactions, physical descriptions, tangential mentions
- **Skip**: Momentary actions, emotional reactions, narrative description, dialogue that doesn't establish facts
