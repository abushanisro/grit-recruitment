import React, { useEffect } from "react";
import { Input, Stack, StackProps } from "@mantine/core";
import { FieldValues, Path, UseFormReturn } from "react-hook-form";
import { get } from "lodash";
import { Editor } from "@tiptap/react";
import { RichTextEditorComponent } from "@components/richTextEditor/RichTextEditorComponent";

function RichTextEditorForm<T extends FieldValues>({
  label,
  hForm,
  name,
  editor,
  setFieldValue,
  ...props
}: {
  hForm: UseFormReturn<T>;
  label: string;
  name: Path<T>;
  editor: Editor | null;
  setFieldValue: (value: string) => void;
} & StackProps) {
  const {
    register,
    setError,
    clearErrors,
    formState: { errors },
  } = hForm;
  const error = get(errors, name);
  useEffect(() => {
    register(name, {
      required: `${name} is required`,
    });
  }, []);
  useEffect(() => {
    if (!editor) {
      return;
    }

    const updateHandler = () => {
      const doesEditorHaveValue = !editor.isEmpty;
      if (doesEditorHaveValue) {
        setFieldValue(JSON.stringify(editor.getJSON()));
        clearErrors(name);
        return;
      }
      setFieldValue("");
      setError(name, {
        type: "custom",
        message: `${name} is required`,
      });
    };

    editor.on("update", updateHandler);

    return () => {
      editor.off("update", updateHandler);
    };
  }, [editor]);
  return (
    <Stack gap={10} {...props}>
      <Input.Label
        required={true}
        fw={700}
        fz={17}
        c="secondaryGreen.1"
        style={{ wordBreak: "break-word" }}
      >
        {" "}
        {label}
      </Input.Label>
      <RichTextEditorComponent isReadonly={false} editor={editor} />
      {error && (
        <>
          <input type="hidden" {...register(name)} />
          <Input.Error fz={{ base: 12, sm: 14 }}>
            {error.message ? String(error.message) : ""}
          </Input.Error>
        </>
      )}
    </Stack>
  );
}

export default RichTextEditorForm;
