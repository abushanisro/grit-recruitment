import "@mantine/tiptap/styles.css";
import { RichTextEditor } from "@mantine/tiptap";
import { Editor } from "@tiptap/react";
import { ScrollArea,Text} from "@mantine/core";
import { limit } from "contract/common";

export const RichTextEditorComponent = ({
  editor,
  isReadonly,
}: {
  editor: Editor | null;

  isReadonly: boolean;
  }) => {
  const charactersRemaining =
    limit - editor?.storage.characterCount.characters();

  const predictCharacterColor = () => {
    const percentage = (charactersRemaining / limit) * 100;
    if (percentage < 20) {
      return "red";
    } else if (percentage >= 20 && percentage <= 60) {
      return "orange";
    } else {
      return "green";
    }
  };
  return (
    <>
      <RichTextEditor
        style={{
          border: !isReadonly
            ? "2px solid var(--mantine-color-customGray-5)"
            : "none",
          marginLeft: !isReadonly ? 0 : "-14px",
        }}
        editor={editor}
      >
        {!isReadonly && (
          <RichTextEditor.Toolbar
            style={{ zIndex: 1 }}
            bg={"linear-gradient(296deg, #1B264F 0.43%, #011C1E 95.48%)"}
            sticky
            stickyOffset={60}
          >
            <RichTextEditor.ControlsGroup
              style={{ borderRadius: "4px" }}
              c="black"
            >
              <RichTextEditor.Bold />
              <RichTextEditor.Italic />
              <RichTextEditor.Underline />
              <RichTextEditor.Strikethrough />
              <RichTextEditor.ClearFormatting />
              <RichTextEditor.Highlight />
              <RichTextEditor.Code />
            </RichTextEditor.ControlsGroup>

            <RichTextEditor.ControlsGroup
              style={{ borderRadius: "4px" }}
              c="black"
            >
              <RichTextEditor.H1 />
              <RichTextEditor.H2 />
              <RichTextEditor.H3 />
              <RichTextEditor.H4 />
            </RichTextEditor.ControlsGroup>

            <RichTextEditor.ControlsGroup
              style={{ borderRadius: "4px" }}
              c="black"
            >
              <RichTextEditor.Hr />
              <RichTextEditor.BulletList />
              <RichTextEditor.OrderedList />
              <RichTextEditor.Subscript />
              <RichTextEditor.Superscript />
            </RichTextEditor.ControlsGroup>

            <RichTextEditor.ControlsGroup
              style={{ borderRadius: "4px" }}
              c="black"
            >
              <RichTextEditor.Link />
              <RichTextEditor.Unlink />
            </RichTextEditor.ControlsGroup>

            <RichTextEditor.ControlsGroup
              style={{ borderRadius: "4px" }}
              c="black"
            >
              <RichTextEditor.AlignLeft />
              <RichTextEditor.AlignCenter />
              <RichTextEditor.AlignJustify />
              <RichTextEditor.AlignRight />
            </RichTextEditor.ControlsGroup>

            <RichTextEditor.ControlsGroup
              style={{ borderRadius: "4px" }}
              c="black"
            >
              <RichTextEditor.Undo />
              <RichTextEditor.Redo />
            </RichTextEditor.ControlsGroup>
          </RichTextEditor.Toolbar>
        )}
        {!isReadonly ? (
          <ScrollArea pr={10} py={10} h={180}>
            <RichTextEditor.Content bg="transparent" c="white" />
          </ScrollArea>
        ) : (
          <RichTextEditor.Content bg="transparent" c="white" />
        )}
      </RichTextEditor>
      {!isReadonly && (
        <Text fz={{ base: 12, sm: 14 }} ta="right" c={predictCharacterColor()}>
          {charactersRemaining} characters remaining of total {limit} characters
        </Text>
      )}
    </>
  );
};
