import { z } from "zod";
import Highlight from "@tiptap/extension-highlight";
import { StarterKit } from "@tiptap/starter-kit";
import Underline from "@tiptap/extension-underline";
import TextAlign from "@tiptap/extension-text-align";
import Superscript from "@tiptap/extension-superscript";
import SubScript from "@tiptap/extension-subscript";
import { Link } from "@mantine/tiptap";
import CharacterCount from "@tiptap/extension-character-count";
export const SuccessSchema = z.object({
  isSuccess: z.boolean(),
  message: z.string(),
});

export const FileUploadErrorResponseSchema = z.object({
  errors: z.string().array(),
  kind: z.literal("error"),
});

export const limit = 2000;
export const tipTapExtensions = [
  StarterKit,
  Underline,
  Link,
  Superscript,
  SubScript,
  Highlight,
  TextAlign.configure({ types: ["heading", "paragraph"] }),
  CharacterCount.configure({
    limit,
  }),
];
