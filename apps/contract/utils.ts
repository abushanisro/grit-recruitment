import { z } from "zod";
import { ExperienceInYear } from "./enum";

export function createPaginatedResponseSchema<ItemType extends z.ZodTypeAny>(
  itemSchema: ItemType
) {
  return z.object({
    currentPageNumber: z.number(),
    currentPageSize: z.number(),
    totalItems: z.number(),
    totalPages: z.number(),
    results: z.array(itemSchema),
  });
}

export function createPaginatedResponseSchemaForManageApplicantCount<
  ItemType extends z.ZodTypeAny
>(itemSchema: ItemType) {
  return z.object({
    currentPageNumber: z.number(),
    currentPageSize: z.number(),
    pendingCount: z.number(),
    shortlistedCount: z.number(),
    rejectedCount: z.number(),
    totalPages: z.number(),
    results: z.array(itemSchema),
  });
}

export const generateSlugForIdPagePath = (paths: string[]) => {
  return encodeURIComponent(
    paths.join("-").replaceAll("/", "-").split(" ").join("-")
  );
};

export function booleanStringSchema(fieldName: string) {
  return z
    .union([z.boolean(), z.string()])
    .refine(
      (val) => {
        if (typeof val === "string") {
          return val.toLowerCase() === "true" || val.toLowerCase() === "false";
        }
        return typeof val === "boolean";
      },
      {
        message: `${fieldName} should be a boolean or a string that can be converted to a boolean`,
      }
    )
    .transform((val) => {
      if (typeof val === "string") {
        return val.toLowerCase() === "true";
      }
      return val;
    });
}

export function dateStringSchema(fieldName: string) {
  return z
    .string()
    .refine((val) => !isNaN(Date.parse(val)), {
      message: `${fieldName} should be a valid date string`,
    })
    .transform((val) => new Date(val));
}

export function getExperienceInYearsFromEnum(value: ExperienceInYear) {
  switch (value) {
    case ExperienceInYear.Fresher:
      return 0;
    case ExperienceInYear.OneYears:
      return 1;
    case ExperienceInYear.TwoYears:
      return 2;
    case ExperienceInYear.ThreeYears:
      return 3;
    case ExperienceInYear.FourYears:
      return 4;
    case ExperienceInYear.FiveYears:
      return 5;
    case ExperienceInYear.SixYears:
      return 6;
    case ExperienceInYear.SevenYears:
      return 7;
    case ExperienceInYear.EightYears:
      return 8;
    case ExperienceInYear.NineYears:
      return 9;
    case ExperienceInYear.TenYears:
      return 10;
    case ExperienceInYear.MoreThanTenYears:
      return 11;
  }
}
