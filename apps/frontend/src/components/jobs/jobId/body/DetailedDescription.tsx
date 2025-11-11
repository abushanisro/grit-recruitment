import { Divider, Grid } from "@mantine/core";
import SubHeading from "@/components/jobs/jobId/body/SubHeading";

import { JobIdResultsType } from "@/types/jobs";
import { RichTextEditorComponent } from "@components/richTextEditor/RichTextEditorComponent";

import { useEditor } from "@tiptap/react";
import { richTextEditorExtension } from "@/data/commonExtensions";

const JobIdBodyDetailedDescription = ({
  data,
}: {
  data: { body: JobIdResultsType };
  }) => {
    const content = data.body.description;

    const editor = useEditor({
      extensions: richTextEditorExtension,
      content,
      editable: true,
    });
  return (
    <Grid.Col span={{ base: 12, md: 7, xl: 8 }}>
      {data.body.description && (
        <>
          <SubHeading label="Job description" />
          <Divider mt={18} mb={28} />
          <RichTextEditorComponent isReadonly={true} editor={editor} />
        </>
      )}
    </Grid.Col>
  );
};

export default JobIdBodyDetailedDescription;
